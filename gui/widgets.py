"""
Widget personalizzati per GUI
"""

import tkinter as tk
from tkinter import ttk
from .styles import AppStyles

class StatusBar(tk.Frame):
    """Barra di stato in basso"""
    
    def __init__(self, parent):
        super().__init__(parent, relief=tk.SUNKEN, borderwidth=1)
        
        self.label = tk.Label(
            self,
            text="Pronto",
            anchor=tk.W,
            font=AppStyles.FONTS['default']
        )
        self.label.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5, pady=2)
    
    def set_text(self, text):
        """Aggiorna testo nella status bar"""
        self.label.config(text=text)
        self.update_idletasks()

class ProgressFrame(tk.Frame):
    """Frame con progress bar determinata e percentuale"""
    
    def __init__(self, parent):
        super().__init__(parent)
        
        # Label status
        self.label = tk.Label(
            self,
            text="",
            font=('Segoe UI', 9)
        )
        self.label.pack(side=tk.TOP, anchor=tk.W, pady=(0, 5))
        
        # Container per progress bar e percentuale
        progress_container = tk.Frame(self)
        progress_container.pack(fill='x')
        
        # Progress bar DETERMINATA (con percentuale)
        self.progress = ttk.Progressbar(
            progress_container,
            mode='determinate',  # ✅ CAMBIATO da 'indeterminate'
            length=400
        )
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Label percentuale
        self.percent_label = tk.Label(
            progress_container,
            text="0%",
            font=('Segoe UI', 10, 'bold'),
            foreground='#2196F3',
            width=6
        )
        self.percent_label.pack(side=tk.RIGHT, padx=(10, 0))
        
        self.is_running = False
        self.current_value = 0
        self.max_value = 100
    
    def start(self, text="Elaborazione in corso..."):
        """Avvia progress bar"""
        self.label.config(text=text)
        self.progress['value'] = 0
        self.percent_label.config(text="0%")
        self.current_value = 0
        self.is_running = True
        self.pack(fill='x', padx=10, pady=5)
    
    def update_progress(self, current, total):
        """Aggiorna progresso con valore assoluto"""
        if total > 0 and self.is_running:
            percent = min(100, int((current / total) * 100))
            self.progress['value'] = percent
            self.percent_label.config(text=f"{percent}%")
            self.current_value = current
            self.max_value = total
            self.update_idletasks()
    
    def set_percentage(self, percent):
        """Imposta percentuale diretta"""
        percent = max(0, min(100, int(percent)))
        self.progress['value'] = percent
        self.percent_label.config(text=f"{percent}%")
        self.update_idletasks()
    
    def set_text(self, text):
        """Aggiorna testo label"""
        self.label.config(text=text)
        self.update_idletasks()
    
    def stop(self):
        """Ferma progress bar"""
        self.is_running = False
        self.progress['value'] = 100
        self.percent_label.config(text="100%")
        self.update_idletasks()

class ScrollableFrame(tk.Frame):
    """Frame scrollabile"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, **kwargs)
        
        # Canvas + Scrollbar
        self.canvas = tk.Canvas(self, borderwidth=0, background="#ffffff")
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas, background="#ffffff")
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        # Mouse wheel binding
        self.bind_mouse_wheel()
    
    def bind_mouse_wheel(self):
        """Abilita scroll con rotella mouse"""
        def _on_mousewheel(event):
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        
        self.canvas.bind_all("<MouseWheel>", _on_mousewheel)

class CollapsibleFrame(tk.Frame):
    """Frame collassabile/espandibile"""
    
    def __init__(self, parent, title="", **kwargs):
        super().__init__(parent, **kwargs)
        
        self.is_collapsed = False
        
        # Header con pulsante toggle
        self.header = tk.Frame(self, relief=tk.RAISED, borderwidth=1)
        self.header.pack(fill=tk.X, pady=(0, 2))
        
        self.toggle_button = tk.Button(
            self.header,
            text="▼ " + title,
            command=self.toggle,
            font=AppStyles.FONTS['heading'],
            anchor=tk.W,
            relief=tk.FLAT,
            cursor='hand2'
        )
        self.toggle_button.pack(fill=tk.X, padx=5, pady=5)
        
        # Content frame
        self.content_frame = tk.Frame(self)
        self.content_frame.pack(fill=tk.BOTH, expand=True)
    
    def toggle(self):
        """Toggle collapse/expand"""
        if self.is_collapsed:
            self.content_frame.pack(fill=tk.BOTH, expand=True)
            self.toggle_button.config(text="▼ " + self.toggle_button.cget("text")[2:])
        else:
            self.content_frame.pack_forget()
            self.toggle_button.config(text="▶ " + self.toggle_button.cget("text")[2:])
        
        self.is_collapsed = not self.is_collapsed

class StatsPanel(tk.LabelFrame):
    """Panel per visualizzare statistiche"""
    
    def __init__(self, parent, title="Statistiche"):
        super().__init__(
            parent,
            text=title,
            font=AppStyles.FONTS['heading'],
            padx=10,
            pady=10
        )
        
        self.stats = {}
        self.labels = {}
    
    def add_stat(self, key, label, value="0"):
        """Aggiunge una statistica"""
        row = len(self.stats)
        
        # Label
        label_widget = tk.Label(
            self,
            text=label + ":",
            font=AppStyles.FONTS['default'],
            anchor=tk.W
        )
        label_widget.grid(row=row, column=0, sticky=tk.W, pady=2)
        
        # Value
        value_widget = tk.Label(
            self,
            text=str(value),
            font=AppStyles.FONTS['default'],
            anchor=tk.E,
            fg=AppStyles.COLORS['primary']
        )
        value_widget.grid(row=row, column=1, sticky=tk.E, pady=2, padx=(10, 0))
        
        self.stats[key] = value
        self.labels[key] = value_widget
    
    def update_stat(self, key, value):
        """Aggiorna una statistica"""
        if key in self.labels:
            self.stats[key] = value
            self.labels[key].config(text=str(value))
    
    def reset_stats(self):
        """Reset tutte le statistiche"""
        for key in self.stats:
            self.update_stat(key, 0)

class LogViewer(tk.Frame):
    """Visualizzatore log con colori"""
    
    def __init__(self, parent):
        super().__init__(parent)
        
        # Text widget con scrollbar
        scrollbar = tk.Scrollbar(self)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.text = tk.Text(
            self,
            wrap=tk.WORD,
            yscrollcommand=scrollbar.set,
            font=AppStyles.FONTS['console'],
            height=15,
            state=tk.DISABLED
        )
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.text.yview)
        
        # Configura tag per colori
        self.text.tag_config('ERROR', foreground=AppStyles.COLORS['error'])
        self.text.tag_config('WARNING', foreground=AppStyles.COLORS['warning'])
        self.text.tag_config('INFO', foreground=AppStyles.COLORS['text'])
        self.text.tag_config('DEBUG', foreground=AppStyles.COLORS['text_secondary'])
        self.text.tag_config('SUCCESS', foreground=AppStyles.COLORS['success'])
    
    def append(self, text, level='INFO'):
        """Aggiunge testo con colore basato su livello"""
        self.text.config(state=tk.NORMAL)
        self.text.insert(tk.END, text + '\n', level)
        self.text.see(tk.END)
        self.text.config(state=tk.DISABLED)
    
    def clear(self):
        """Pulisce il log"""
        self.text.config(state=tk.NORMAL)
        self.text.delete(1.0, tk.END)
        self.text.config(state=tk.DISABLED)