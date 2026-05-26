"""
gui/login_window.py — Finestra di login modale mostrata all'avvio dell'app
se non esiste una sessione valida in jwt_store.

Flow:
  avvio → se token locale valido → skip login → main window
        → se token scaduto ma server raggiungibile → tenta refresh silenzioso
        → se offline ma token c'è e offline_ok=True → avvio in read-only
        → altrimenti → mostra questa finestra

L'utente inserisce email + password, può modificare l'URL server e salvare
la scelta "ricordami email". Dopo login riuscito, chiude e ritorna True.
"""
from __future__ import annotations
import threading
from tkinter import messagebox
from typing import Optional

import customtkinter as ctk

from config.app_config import config as client_config, save as save_client_config
from services.api_client import (
    ApiClient, AuthError, ServerUnreachableError, ApiError,
)
# v1092.0 (R6.1 fase 2): i18n
from services.i18n import t


# Colori — allineati alla palette dell'app
PALETTE = {
    "bg":         "#0f1419",
    "surface":    "#1e2533",
    "surface2":   "#2a3344",
    "border":     "#333a4a",
    "text":       "#e8edf2",
    "text_dim":   "#7a8699",
    "primary":    "#3b6fd4",
    "primary_hover": "#2d5ab8",
    "error":      "#d84545",
    "success":    "#50aa70",
}


class LoginWindow:
    """
    Finestra modale di login. Chiama `on_success(api_client, user_info)` al
    completamento e chiude.
    """

    def __init__(self):
        self.result: Optional[dict] = None    # popolato da login riuscito
        self.api_client: Optional[ApiClient] = None

        self.root = ctk.CTk()
        self.root.title(t("login.title_window"))
        self.root.geometry("440x520")
        self.root.resizable(False, False)
        self.root.configure(fg_color=PALETTE["bg"])
        # v0.0.2.2 fix: icona finestra + taskbar
        try:
            from gui.app_icon import set_window_icon
            set_window_icon(self.root)
        except Exception:
            pass
        self._center()

        self._build_ui()

    def _center(self):
        self.root.update_idletasks()
        w, h = 440, 520
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = max(0, (sw - w) // 2)
        y = max(0, (sh - h) // 2)
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    def _build_ui(self):
        # Logo / titolo
        header = ctk.CTkFrame(self.root, fg_color="transparent", height=100)
        header.pack(fill="x", pady=(32, 8))

        # v0.0.2.2: prova a usare taskbar_active.png come logo, fallback emoji 🎵
        _logo_used = False
        try:
            from gui.app_icon import get_title_icon_photo
            _logo = get_title_icon_photo(size=56)
            if _logo is not None:
                ctk.CTkLabel(header, text="", image=_logo).pack()
                header._logo_ref = _logo  # evita GC
                _logo_used = True
        except Exception:
            pass
        if not _logo_used:
            ctk.CTkLabel(header, text="🎵",
                         font=("Segoe UI", 44),
                         text_color=PALETTE["primary"]).pack()

        ctk.CTkLabel(header, text=t("app.name"),
                     font=("Segoe UI", 18, "bold"),
                     text_color=PALETTE["text"]).pack(pady=(6, 0))
        ctk.CTkLabel(header, text=t("login.header"),
                     font=("Segoe UI", 11),
                     text_color=PALETTE["text_dim"]).pack()

        # Form
        form = ctk.CTkFrame(self.root, fg_color=PALETTE["surface"],
                            corner_radius=12)
        form.pack(fill="x", padx=28, pady=(18, 8))

        ctk.CTkLabel(form, text=t("login.field_email"), anchor="w",
                     font=("Segoe UI", 11, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(fill="x", padx=18, pady=(14, 2))
        # v1086.3 round 4: email come StringVar con trace che forza lowercase
        # in tempo reale (mentre l'utente digita). Coerente con il fatto che
        # il server normalizza email a lowercase: vedere "USER@..." nell'UI
        # quando il server riconosce solo "user@..." e' confondente.
        self.email_var = ctk.StringVar(value=(client_config.last_email or "").lower())
        def _force_lower(*_args):
            v = self.email_var.get()
            low = v.lower()
            if v != low:
                # Mantieni la posizione del cursore mentre re-set
                try:
                    pos = self.email_entry.index("insert")
                except Exception:
                    pos = None
                self.email_var.set(low)
                if pos is not None:
                    try:
                        self.email_entry.icursor(pos)
                    except Exception:
                        pass
        self.email_var.trace_add("write", _force_lower)
        self.email_entry = ctk.CTkEntry(
            form, textvariable=self.email_var,
            fg_color=PALETTE["surface2"], border_color=PALETTE["border"],
            text_color=PALETTE["text"], height=36,
        )
        self.email_entry.pack(fill="x", padx=18, pady=(0, 10))

        ctk.CTkLabel(form, text=t("login.field_password"), anchor="w",
                     font=("Segoe UI", 11, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(fill="x", padx=18, pady=(4, 2))
        self.password_var = ctk.StringVar()
        self.password_entry = ctk.CTkEntry(
            form, textvariable=self.password_var, show="•",
            fg_color=PALETTE["surface2"], border_color=PALETTE["border"],
            text_color=PALETTE["text"], height=36,
        )
        self.password_entry.pack(fill="x", padx=18, pady=(0, 10))

        # Remember me
        self.remember_var = ctk.BooleanVar(value=client_config.remember_email)
        ctk.CTkCheckBox(form, text=t("login.chk_remember_email"),
                        variable=self.remember_var,
                        font=("Segoe UI", 10),
                        text_color=PALETTE["text_dim"],
                        fg_color=PALETTE["primary"],
                        hover_color=PALETTE["primary_hover"],
                        checkbox_width=16, checkbox_height=16,
                        ).pack(anchor="w", padx=18, pady=(0, 14))

        # Bottone Login
        self.login_btn = ctk.CTkButton(
            form, text=t("login.btn_login"),
            font=("Segoe UI", 12, "bold"),
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            text_color="#ffffff", height=40, corner_radius=8,
            command=self._do_login,
        )
        self.login_btn.pack(fill="x", padx=18, pady=(0, 10))
        self.root.bind("<Return>", lambda e: self._do_login())

        # v0.0.2.3: Link registrazione (nascosto se admin ha disabilitato)
        self._signup_row = ctk.CTkFrame(form, fg_color="transparent")
        self._signup_row.pack(fill="x", padx=18, pady=(0, 14))
        ctk.CTkLabel(self._signup_row, text=t("login.no_account_prompt"),
                     font=("Segoe UI", 10),
                     text_color=PALETTE["text_dim"]).pack(side="left")
        signup_lbl = ctk.CTkLabel(
            self._signup_row, text=t("login.btn_register"),
            font=("Segoe UI", 10, "bold"),
            text_color=PALETTE["primary"], cursor="hand2")
        signup_lbl.pack(side="left", padx=(4, 0))
        signup_lbl.bind("<Button-1>", lambda e: self._open_register_dialog())

        # v1085g: Check se la registrazione è abilitata sul server.
        # Se disabilitata, nascondi il link e mostra il messaggio admin.
        # Eseguito in thread separato per non bloccare la UI all'apertura.
        self.root.after(200, self._check_registration_enabled)

        # Status label
        self.status_var = ctk.StringVar(value="")
        self.status_label = ctk.CTkLabel(
            self.root, textvariable=self.status_var,
            font=("Segoe UI", 10),
            text_color=PALETTE["text_dim"],
            wraplength=380, justify="center",
        )
        self.status_label.pack(pady=(2, 6))

        # Sezione "avanzate" — URL server
        adv = ctk.CTkFrame(self.root, fg_color="transparent")
        adv.pack(fill="x", padx=28, pady=(6, 16))
        ctk.CTkLabel(adv, text=t("login.field_server"),
                     font=("Segoe UI", 9),
                     text_color=PALETTE["text_dim"]).pack(anchor="w")
        self.server_var = ctk.StringVar(value=client_config.server_url)
        self.server_entry = ctk.CTkEntry(
            adv, textvariable=self.server_var,
            fg_color=PALETTE["surface"], border_color=PALETTE["border"],
            text_color=PALETTE["text_dim"], height=28,
            font=("Consolas", 10),
        )
        self.server_entry.pack(fill="x", pady=(2, 0))

        # Focus iniziale — se email già salvata, va su password
        if client_config.last_email:
            self.password_entry.focus()
        else:
            self.email_entry.focus()

    def _set_status(self, text: str, color_key: str = "text_dim"):
        self.status_var.set(text)
        self.status_label.configure(text_color=PALETTE[color_key])
        self.root.update_idletasks()

    def _set_loading(self, is_loading: bool):
        if is_loading:
            self.login_btn.configure(text=t("login.btn_login_loading"), state="disabled")
        else:
            self.login_btn.configure(text=t("login.btn_login"), state="normal")

    def _do_login(self):
        # v1086.3 round 4: email forzata a lowercase prima del confronto.
        # Pedro feedback: "mi sembra che sia sensitive cap la mail".
        # Lo standard RFC 5321 (SMTP) dice che la parte locale TEORICAMENTE
        # puo' essere case-sensitive, ma in pratica nessun mail provider
        # serio lo e' (Gmail, Outlook, Yahoo trattano "User@..." == "user@...").
        # Quindi normalizziamo lowercase per evitare frustrazione utente.
        # La password resta case-sensitive (giusta).
        email    = self.email_var.get().strip().lower()
        password = self.password_var.get()
        server   = self.server_var.get().strip()

        if not email or not password:
            self._set_status(t("login.err_fill_required"), "error")
            return
        if not server.startswith(("http://", "https://")):
            self._set_status(t("login.err_invalid_url_format"), "error")
            return

        # Salva preferenze client
        client_config.server_url    = server
        client_config.remember_email = self.remember_var.get()
        client_config.last_email     = email if self.remember_var.get() else ""
        save_client_config(client_config)

        # Esegui login in thread per non bloccare UI
        self._set_loading(True)
        self._set_status(t("login.status_connecting"), "text_dim")
        threading.Thread(target=self._login_worker,
                         args=(server, email, password),
                         daemon=True).start()

    def _login_worker(self, server: str, email: str, password: str):
        try:
            client = ApiClient(server)
            resp = client.login(email, password)
            self.api_client = client
            self.result = resp.user
            # Torna sul main thread per chiudere la finestra
            self.root.after(0, self._on_success)
        except AuthError as e:
            err_str = str(e)
            self.root.after(0, lambda: self._on_error(
                t("login.err_invalid_credentials"), detail=err_str))
        except ServerUnreachableError:
            self.root.after(0, lambda: self._on_error(
                t("login.err_server_unreachable_long", url=server),
                detail=""))
        except ApiError as e:
            err_status = e.status
            err_detail = str(e.detail)
            self.root.after(0, lambda: self._on_error(
                t("login.err_server_http", status=err_status),
                detail=err_detail))
        except Exception as e:
            err_str = str(e)
            self.root.after(0, lambda: self._on_error(
                t("login.err_unexpected"), detail=err_str))

    def _check_registration_enabled(self):
        """v1085g: chiede al server se la registrazione self-service è
        abilitata. Se no, nasconde il link 'Registrati' e mostra in
        sostituzione un breve avviso. Tutto in thread separato per
        non bloccare la GUI.
        """
        import threading
        server = self.server_var.get().strip()
        if not server.startswith(("http://", "https://")):
            return  # niente server valido, lascia il link visibile

        def _worker():
            try:
                client = ApiClient(server)
                resp = client.get_registration_status()
                enabled = bool(resp.get("enabled", True))
                if enabled:
                    return  # link già visibile, niente da fare
                # Disabilitata: nascondi il signup_row e mostra avviso
                def _ui():
                    try:
                        for w in self._signup_row.winfo_children():
                            w.destroy()
                        ctk.CTkLabel(
                            self._signup_row,
                            text=t("login.registration_disabled_notice"),
                            font=("Segoe UI", 9),
                            text_color=PALETTE["text_dim"],
                            justify="center"
                        ).pack(fill="x")
                    except Exception:
                        pass
                self.root.after(0, _ui)
            except Exception:
                # Server irraggiungibile o vecchio (no endpoint registration/status):
                # silenzio, lascia il link normale
                pass

        threading.Thread(target=_worker, daemon=True).start()

    def _on_success(self):
        self._set_loading(False)
        self._set_status(t("login.status_logged_in"), "success")
        # v1085c: cancella tutte le after callback pending (es. DPI
        # scaling di customtkinter) prima di destroy. Senza questo, in
        # CMD compaiono messaggi 'invalid command name "...update"'.
        def _safe_destroy():
            try:
                # Tk mantiene lista di after-id in ".__after"; cancelliamo tutto
                for aid in self.root.tk.call('after', 'info'):
                    try:
                        self.root.after_cancel(aid)
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                self.root.destroy()
            except Exception:
                pass
        self.root.after(400, _safe_destroy)

    def _on_error(self, message: str, detail: str = ""):
        self._set_loading(False)
        self._set_status(message, "error")
        if detail:
            # Log ma non mostrare dettagli tecnici all'utente
            print(f"[LoginWindow] {message} — dettaglio: {detail}")

    def show(self) -> tuple[Optional[ApiClient], Optional[dict]]:
        """Blocca finché l'utente non si autentica o chiude la finestra.
        Ritorna (api_client, user_info) o (None, None) se chiusa."""
        self.root.mainloop()
        return self.api_client, self.result

    # ── v0.0.2.3 — Dialog registrazione nuovo utente ──────────────
    def _open_register_dialog(self):
        """
        Dialog modale per la registrazione self-service.

        v1088.4:
        - Finestra Windows STANDALONE (no overrideredirect, no titlebar
          custom): titlebar nativa Windows con icona app, entry in
          taskbar, gestione minimize/restore standard.
        - transient(self.root) + grab_set(): modale figlia della login.
          Windows mostra il blink/ding nativo se l'utente tenta di
          cliccare la login mentre questa è aperta.
        - DLG_H aumentato 540 → 600 per evitare che il campo "Conferma
          password" venga compresso dal pack manager.

        Storico (v1085d): aveva titlebar custom + overrideredirect=True,
        ma quel pattern soffriva degli stessi problemi del dialog
        "Crea utente" admin (BUG-04 in v1086.x): finestra senza bordo
        che ereditava lo z-order della parent → si nascondeva sotto al
        cambio focus.
        """
        from tkinter import messagebox
        import threading

        server = self.server_var.get().strip()
        if not server.startswith(("http://", "https://")):
            messagebox.showerror(t("login.err_invalid_url"),
                                 t("login.err_invalid_url_body"))
            return

        # DLG_H 560: sweet spot empirico osservato sul rendering
        # Windows (DPI scaling 125% rende CTkLabel/CTkEntry leggermente
        # piu' alti dei pixel dichiarati). Sotto i 540 il pack comprime
        # l'ultimo entry "Conferma password", sopra i 580 compare un
        # buco vuoto sotto i bottoni. 560 = bilanciamento corretto.
        DLG_W, DLG_H = 460, 560

        win = ctk.CTkToplevel(self.root)
        win.title(t("register.title_window"))
        win.geometry(f"{DLG_W}x{DLG_H}")
        win.resizable(False, False)
        win.configure(fg_color=PALETTE["bg"])

        # Icona nativa con retry: iconbitmap() su CTkToplevel su Windows
        # ha un timing problematico (la finestra non è ancora "realized"
        # quando la chiamiamo subito dopo la creazione, e l'icona non
        # si attacca alla titlebar). Stesso pattern usato in
        # main_window._set_win_icon (v1076): chiamata immediata +
        # secondo tentativo dopo 250ms quando la finestra è mappata.
        def _apply_icon():
            try:
                if not win.winfo_exists():
                    return
                from gui.app_icon import set_window_icon
                set_window_icon(win)
            except Exception:
                pass
        _apply_icon()
        try:
            win.after(250, _apply_icon)
        except Exception:
            pass

        # transient + grab_set = finestra figlia modale. Windows fa
        # automaticamente il blink/ding sulla titlebar nativa se
        # l'utente clicca la login mentre questa è aperta.
        win.transient(self.root)
        try:
            win.grab_set()
        except Exception:
            pass

        # Centra sopra la login window
        self.root.update_idletasks()
        lx = self.root.winfo_x(); ly = self.root.winfo_y()
        lw = self.root.winfo_width(); lh = self.root.winfo_height()
        win.geometry(f"{DLG_W}x{DLG_H}+{lx + (lw-DLG_W)//2}+{ly + (lh-DLG_H)//2}")

        # Porta la finestra in primo piano subito (evita che parta
        # nascosta dietro altre app del desktop).
        try:
            win.lift()
            win.focus_force()
        except Exception:
            pass

        # ── 2. Btn row (BOTTOM) — pinnato PRIMA del body ─────────────
        btn_row = ctk.CTkFrame(win, fg_color="transparent", height=70)
        btn_row.pack(side="bottom", fill="x", padx=24, pady=(0, 16))
        btn_row.pack_propagate(False)

        # Status label sopra i bottoni (dentro un frame che resta in fondo prima)
        status_var = ctk.StringVar(value="")
        status_frm = ctk.CTkFrame(win, fg_color="transparent", height=36)
        status_frm.pack(side="bottom", fill="x", padx=20, pady=(2, 0))
        status_frm.pack_propagate(False)
        status_lbl = ctk.CTkLabel(status_frm, textvariable=status_var,
                                  font=("Segoe UI", 10),
                                  text_color=PALETTE["text_dim"],
                                  wraplength=400, justify="center")
        status_lbl.pack(expand=True)

        def _set_status(msg: str, color_key: str = "text_dim"):
            status_var.set(msg)
            status_lbl.configure(text_color=PALETTE.get(color_key, PALETTE["text_dim"]))

        # ── 3. Body (riempie il resto, in mezzo) ─────────────────────
        body = ctk.CTkFrame(win, fg_color="transparent")
        body.pack(side="top", fill="both", expand=True, padx=0, pady=0)

        ctk.CTkLabel(body, text=t("register.header"),
                     font=("Segoe UI", 14, "bold"),
                     text_color=PALETTE["text"]
                     ).pack(pady=(16, 2))
        ctk.CTkLabel(body,
                     text=t("register.subtitle"),
                     font=("Segoe UI", 10),
                     text_color=PALETTE["text_dim"],
                     justify="center"
                     ).pack(pady=(0, 12))

        form = ctk.CTkFrame(body, fg_color=PALETTE["surface"], corner_radius=10)
        form.pack(fill="x", padx=24, pady=(0, 4))

        def _field(label: str, show: Optional[str] = None):
            ctk.CTkLabel(form, text=label, anchor="w",
                         font=("Segoe UI", 10, "bold"),
                         text_color=PALETTE["text"]
                         ).pack(fill="x", padx=14, pady=(8, 2))
            var = ctk.StringVar()
            kw = {"show": show} if show else {}
            ctk.CTkEntry(form, textvariable=var,
                         fg_color=PALETTE["surface2"],
                         border_color=PALETTE["border"],
                         text_color=PALETTE["text"], height=32, **kw
                         ).pack(fill="x", padx=14, pady=(0, 4))
            return var

        email_var    = _field(t("register.field_email"))
        username_var = _field(t("register.field_username"))
        pwd_var      = _field(t("register.field_password"), show="•")
        cnf_var      = _field(t("register.field_confirm_password"), show="•")
        # Padding finale del form
        ctk.CTkFrame(form, fg_color="transparent", height=8).pack()

        def _submit():
            email = email_var.get().strip()
            uname = username_var.get().strip()
            pwd   = pwd_var.get()
            cnf   = cnf_var.get()
            if not email or not uname or not pwd:
                _set_status(t("register.err_fill_all"), "error"); return
            if "@" not in email or "." not in email.split("@")[-1]:
                _set_status(t("register.err_invalid_email"), "error"); return
            if len(pwd) < 8:
                _set_status(t("register.err_password_short"), "error"); return
            if pwd != cnf:
                _set_status(t("register.err_password_mismatch"), "error"); return

            btn_ok.configure(text=t("register.btn_submitting"), state="disabled")
            btn_cancel.configure(state="disabled")
            _set_status(t("register.status_sending"), "text_dim")

            def _worker():
                try:
                    client = ApiClient(server)
                    user = client.register(email=email, username=uname, password=pwd)
                    self.root.after(0, lambda: (
                        win.destroy(),
                        self.email_var.set(user["email"]),
                        self.password_var.set(""),
                        self.password_entry.focus(),
                        self._set_status(t("register.status_success"), "success"),
                    ))
                except ApiError as e:
                    if e.status == 409:
                        msg = t("register.err_email_taken")
                    elif e.status == 422:
                        msg = t("register.err_invalid_data")
                    else:
                        msg = t("register.err_server_generic", status=e.status)
                    self.root.after(0, lambda: (
                        _set_status(msg, "error"),
                        btn_ok.configure(text=t("register.btn_submit"), state="normal"),
                        btn_cancel.configure(state="normal"),
                    ))
                except ServerUnreachableError:
                    self.root.after(0, lambda: (
                        _set_status(t("login.err_server_unreachable_short", url=server), "error"),
                        btn_ok.configure(text=t("register.btn_submit"), state="normal"),
                        btn_cancel.configure(state="normal"),
                    ))
                except Exception as e:
                    err_str = str(e)
                    self.root.after(0, lambda: (
                        _set_status(t("register.err_unknown", detail=err_str), "error"),
                        btn_ok.configure(text=t("register.btn_submit"), state="normal"),
                        btn_cancel.configure(state="normal"),
                    ))
            threading.Thread(target=_worker, daemon=True).start()

        btn_cancel = ctk.CTkButton(
            btn_row, text=t("common.btn_cancel"), width=110, height=38,
            fg_color="transparent", hover_color=PALETTE["surface"],
            text_color=PALETTE["text_dim"],
            font=("Segoe UI", 10), command=win.destroy,
        )
        btn_cancel.pack(side="right", padx=(4, 0), pady=14)
        btn_ok = ctk.CTkButton(
            btn_row, text=t("register.btn_submit"), width=170, height=38,
            fg_color=PALETTE["primary"], hover_color=PALETTE["primary_hover"],
            text_color="#ffffff",
            font=("Segoe UI", 11, "bold"), command=_submit,
        )
        btn_ok.pack(side="right", pady=14)
        win.bind("<Return>", lambda e: _submit())
        win.bind("<Escape>", lambda e: win.destroy())


# ── Entry point utile per test ────────────────────────────────────
if __name__ == "__main__":
    w = LoginWindow()
    client, user = w.show()
    print(f"login result: client={client}, user={user}")
