import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';

// Список доступных тем
export const THEMES = [
    { id: 'okx', name: 'OKX Green', icon: '🌿' },
    { id: 'dark', name: 'Neon Lime', icon: '🌙' },
    { id: 'binance', name: 'Binance Gold', icon: '💛' },
    { id: 'ocean', name: 'Ocean Blue', icon: '🌊' },
    { id: 'sunset', name: 'Sunset Orange', icon: '🌅' },
    { id: 'light', name: 'Light Mode', icon: '☀️' },
] as const;

export type ThemeId = typeof THEMES[number]['id'];

interface ThemeContextType {
    theme: ThemeId;
    themeName: string;
    themeIcon: string;
    setTheme: (id: ThemeId) => void;
    cycleTheme: () => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
    const [theme, setThemeState] = useState<ThemeId>(() => {
        const saved = localStorage.getItem('theme') as ThemeId;
        return THEMES.some(t => t.id === saved) ? saved : 'okx';
    });

    const currentTheme = THEMES.find(t => t.id === theme) || THEMES[0];

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
    }, [theme]);

    const setTheme = (id: ThemeId) => {
        if (THEMES.some(t => t.id === id)) {
            setThemeState(id);
        }
    };

    const cycleTheme = () => {
        const currentIndex = THEMES.findIndex(t => t.id === theme);
        const nextIndex = (currentIndex + 1) % THEMES.length;
        setThemeState(THEMES[nextIndex].id);
    };

    return (
        <ThemeContext.Provider value={{
            theme,
            themeName: currentTheme.name,
            themeIcon: currentTheme.icon,
            setTheme,
            cycleTheme
        }}>
            {children}
        </ThemeContext.Provider>
    );
}

export function useTheme() {
    const context = useContext(ThemeContext);
    if (!context) {
        throw new Error('useTheme must be used within a ThemeProvider');
    }
    return context;
}

