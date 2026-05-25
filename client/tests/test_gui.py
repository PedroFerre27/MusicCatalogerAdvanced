"""
Test rapido per verificare che la GUI si avvii
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def test_gui_imports():
    """Test import moduli GUI"""
    print("Test 1: Import moduli GUI...")
    try:
        from gui.styles import AppStyles
        from gui.widgets import StatusBar, LogViewer, StatsPanel
        from gui.dialogs import AboutDialog, SettingsDialog
        from gui.main_window import TrackLabGUI
        
        print("  ✓ Tutti i moduli GUI importati")
        return True
    except ImportError as e:
        print(f"  ✗ Errore import: {e}")
        return False

def test_tkinter():
    """Test disponibilità tkinter"""
    print("\nTest 2: Tkinter...")
    try:
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()  # Nascondi finestra
        root.destroy()
        
        print("  ✓ Tkinter disponibile")
        return True
    except ImportError:
        print("  ✗ Tkinter non disponibile")
        return False

def test_gui_creation():
    """Test creazione GUI (senza mostrare)"""
    print("\nTest 3: Creazione GUI...")
    try:
        import tkinter as tk
        from gui.main_window import TrackLabGUI
        
        root = tk.Tk()
        root.withdraw()
        
        app = TrackLabGUI(root)
        
        # Verifica componenti
        assert hasattr(app, 'log_viewer')
        assert hasattr(app, 'stats_panel')
        assert hasattr(app, 'status_bar')
        
        root.destroy()
        
        print("  ✓ GUI creata correttamente")
        return True
    except Exception as e:
        print(f"  ✗ Errore creazione GUI: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("TEST GUI MUSIC CATALOGER")
    print("=" * 50)
    
    results = []
    results.append(test_gui_imports())
    results.append(test_tkinter())
    results.append(test_gui_creation())
    
    print("\n" + "=" * 50)
    if all(results):
        print("✓ TUTTI I TEST GUI SUPERATI")
        print("\nPuoi avviare la GUI con:")
        print("  python run_gui.py")
        print("  oppure")
        print("  run_gui.bat  (su Windows)")
    else:
        print("✗ ALCUNI TEST FALLITI")
    print("=" * 50)