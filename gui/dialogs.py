"""
Dialog personalizzati
"""

import tkinter as tk
from tkinter import ttk, messagebox
from .styles import AppStyles

class AboutDialog(tk.Toplevel):
    """Dialog About"""
    
    def __init__(self, parent):
        super().__init__(parent)
        
        self.title("About Music Cataloger Advanced")
        self.resizable(False, False)
        
        # Centra il dialog
        self.geometry("450x350")
        self.transient(parent)
        self.grab_set()
        
        # Content
        main_frame = tk.Frame(self, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Logo/Title
        title_label = tk.Label(
            main_frame,
            text="🎵 Music Cataloger Advanced",
            font=AppStyles.FONTS['title']
        )
        title_label.pack(pady=(0, 10))
        
        version_label = tk.Label(
            main_frame,
            text="Versione 0.0.2.0",
            font=AppStyles.FONTS['default']
        )
        version_label.pack()
        
        # Separator
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=15)
        
        # Description
        desc = """Sistema avanzato e modulare per catalogare, 
organizzare e arricchire collezioni musicali MP3.

Caratteristiche:
- Catalogazione automatica per genere
- Metadati da MusicBrainz, Last.fm, Spotify
- BPM automatico da multiple fonti
- Classificazione speciale musica Latina
- Organizzazione Salsa per difficoltà

Architettura modulare, estensibile, manutenibile."""
        
        desc_label = tk.Label(
            main_frame,
            text=desc,
            font=AppStyles.FONTS['default'],
            justify=tk.LEFT,
            wraplength=400
        )
        desc_label.pack(pady=10)
        
        # Separator
        ttk.Separator(main_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=15)
        
        # Credits
        credits_label = tk.Label(
            main_frame,
            text="© 2025 - Uso personale ed educativo",
            font=AppStyles.FONTS['default'],
            fg=AppStyles.COLORS['text_secondary']
        )
        credits_label.pack()
        
        # Close button
        close_btn = tk.Button(
            main_frame,
            text="Chiudi",
            command=self.destroy,
            **AppStyles.get_button_style()
        )
        close_btn.pack(pady=(15, 0))

class SettingsDialog(tk.Toplevel):
    """Dialog Impostazioni"""
    
    def __init__(self, parent, current_settings):
        super().__init__(parent)
        
        self.title("Impostazioni")
        self.geometry("500x400")
        self.transient(parent)
        self.grab_set()
        
        self.settings = current_settings.copy()
        self.result = None
        
        # Notebook per tabs
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tab Generali
        general_frame = tk.Frame(notebook, padx=20, pady=20)
        notebook.add(general_frame, text="Generali")
        
        # Timeout API
        tk.Label(general_frame, text="Timeout API (secondi):", font=AppStyles.FONTS['default']).grid(
            row=0, column=0, sticky=tk.W, pady=5
        )
        self.timeout_var = tk.IntVar(value=current_settings.get('timeout', 10))
        tk.Spinbox(
            general_frame,
            from_=5,
            to=60,
            textvariable=self.timeout_var,
            width=10
        ).grid(row=0, column=1, sticky=tk.W, pady=5)
        
        # Max retries
        tk.Label(general_frame, text="Tentativi massimi:", font=AppStyles.FONTS['default']).grid(
            row=1, column=0, sticky=tk.W, pady=5
        )
        self.retries_var = tk.IntVar(value=current_settings.get('max_retries', 3))
        tk.Spinbox(
            general_frame,
            from_=1,
            to=10,
            textvariable=self.retries_var,
            width=10
        ).grid(row=1, column=1, sticky=tk.W, pady=5)
        
        # Tab BPM
        bpm_frame = tk.Frame(notebook, padx=20, pady=20)
        notebook.add(bpm_frame, text="BPM")
        
        tk.Label(bpm_frame, text="Range BPM valido:", font=AppStyles.FONTS['heading']).grid(
            row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 10)
        )
        
        tk.Label(bpm_frame, text="Minimo:", font=AppStyles.FONTS['default']).grid(
            row=1, column=0, sticky=tk.W, pady=5
        )
        self.bpm_min_var = tk.IntVar(value=current_settings.get('bpm_min', 60))
        tk.Spinbox(
            bpm_frame,
            from_=40,
            to=100,
            textvariable=self.bpm_min_var,
            width=10
        ).grid(row=1, column=1, sticky=tk.W, pady=5)
        
        tk.Label(bpm_frame, text="Massimo:", font=AppStyles.FONTS['default']).grid(
            row=2, column=0, sticky=tk.W, pady=5
        )
        self.bpm_max_var = tk.IntVar(value=current_settings.get('bpm_max', 200))
        tk.Spinbox(
            bpm_frame,
            from_=150,
            to=250,
            textvariable=self.bpm_max_var,
            width=10
        ).grid(row=2, column=1, sticky=tk.W, pady=5)
        
        # Buttons frame
        btn_frame = tk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        
        tk.Button(
            btn_frame,
            text="Annulla",
            command=self.cancel,
            **AppStyles.get_button_style()
        ).pack(side=tk.RIGHT, padx=5)
        
        tk.Button(
            btn_frame,
            text="Salva",
            command=self.save,
            **AppStyles.get_button_style()
        ).pack(side=tk.RIGHT)
    
    def save(self):
        """Salva impostazioni"""
        self.result = {
            'timeout': self.timeout_var.get(),
            'max_retries': self.retries_var.get(),
            'bpm_min': self.bpm_min_var.get(),
            'bpm_max': self.bpm_max_var.get(),
        }
        self.destroy()
    
    def cancel(self):
        """Annulla"""
        self.result = None
        self.destroy()

def show_error(parent, title, message):
    """Mostra dialog errore"""
    messagebox.showerror(title, message, parent=parent)

def show_warning(parent, title, message):
    """Mostra dialog warning"""
    messagebox.showwarning(title, message, parent=parent)

def show_info(parent, title, message):
    """Mostra dialog info"""
    messagebox.showinfo(title, message, parent=parent)

def ask_yes_no(parent, title, message):
    """Dialog Yes/No"""
    return messagebox.askyesno(title, message, parent=parent)