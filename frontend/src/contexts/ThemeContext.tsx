import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';

// Список доступных тем — сейчас только 2:
//   okx  — основная, safe default
//   pro  — экспериментальная "де-AI'нутая" (IBM Plex + flat widgets + OKX green).
// Остальные темы (dark, binance, ocean, sunset, light, quant, editorial, swiss)
// остаются в index.css как dead code — не удалили, чтобы проще включить обратно.
export const THEMES = [
    { id: 'okx', name: 'OKX Green', icon: '🌿' },
    { id: 'pro', name: 'Pro (тест)', icon: '⚡' },
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

