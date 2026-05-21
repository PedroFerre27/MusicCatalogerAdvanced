"""
Stili e temi per GUI
"""

class AppStyles:
    """Stili centralizzati per l'applicazione"""
    
    # Colori
    COLORS = {
        'primary': '#2196F3',
        'primary_dark': '#1976D2',
        'primary_light': '#BBDEFB',
        'accent': '#FF5722',
        'success': '#4CAF50',
        'warning': '#FF9800',
        'error': '#F44336',
        'text': '#212121',
        'text_secondary': '#757575',
        'background': '#FAFAFA',
        'surface': '#FFFFFF',
        'border': '#E0E0E0',
    }
    
    # Font
    FONTS = {
        'default': ('Segoe UI', 9),
        'title': ('Segoe UI', 16, 'bold'),
        'heading': ('Segoe UI', 12, 'bold'),
        'console': ('Consolas', 9),
        'button': ('Segoe UI', 9, 'bold'),
    }
    
    # Padding
    PADDING = {
        'small': 5,
        'medium': 10,
        'large': 20,
    }
    
    @classmethod
    def get_button_style(cls):
        """Stile per pulsanti"""
        return {
            'font': cls.FONTS['button'],
            'padx': cls.PADDING['medium'],
            'pady': cls.PADDING['small'],
            'relief': 'flat',
            'borderwidth': 0,
            'cursor': 'hand2',
        }
    
    @classmethod
    def get_frame_style(cls):
        """Stile per frame"""
        return {
            'relief': 'flat',
            'borderwidth': 1,
            'background': cls.COLORS['surface'],
        }
    
    @classmethod
    def get_entry_style(cls):
        """Stile per entry"""
        return {
            'font': cls.FONTS['default'],
            'relief': 'solid',
            'borderwidth': 1,
        }