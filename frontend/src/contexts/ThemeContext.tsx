import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';

// Список доступных тем:
//   okx              — основная dark, safe default
//   pro              — экспериментальная "де-AI'нутая"
//   editorial-light  — editorial newspaper, тёплая бумага
//   editorial-dark   — editorial newspaper, тёплый чёрный
// Остальные темы (dark, binance, ocean, sunset, light, quant, swiss)
// остаются в index.css как dead code — не удалили на случай возврата.
export const THEMES = [
    { id: 'okx',              name: 'OKX Green',       icon: '🌿' },
    { id: 'pro',              name: 'Pro (тест)',      icon: '⚡' },
    { id: 'editorial-light',  name: 'Editorial Light', icon: '📰' },
    { id: 'editorial-dark',   name: 'Editorial Dark',  icon: '🖤' },
] as const;

export type ThemeId = typeof THEMES[number]['id'];

// Accent — независимая dimension. По умолчанию 'default' (тема сама решает).
// Pumpkin / Indigo переопределяют --accent через [data-accent="..."] в CSS.
// Имеет смысл с editorial-темами, но работает поверх любой.
export const ACCENTS = [
    { id: 'default', name: 'Тема',     swatch: 'transparent' },
    { id: 'pumpkin', name: 'Pumpkin',  swatch: '#FF5C2B' },
] as const;

export type AccentId = typeof ACCENTS[number]['id'];

interface ThemeContextType {
    theme: ThemeId;
    themeName: string;
    themeIcon: string;
    setTheme: (id: ThemeId) => void;
    cycleTheme: () => void;
    accent: AccentId;
    accentName: string;
    setAccent: (id: AccentId) => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
    const [theme, setThemeState] = useState<ThemeId>(() => {
        const saved = localStorage.getItem('theme') as ThemeId;
        return THEMES.some(t => t.id === saved) ? saved : 'okx';
    });

    const [accent, setAccentState] = useState<AccentId>(() => {
        const saved = localStorage.getItem('accent') as AccentId;
        return ACCENTS.some(a => a.id === saved) ? saved : 'default';
    });

    const currentTheme = THEMES.find(t => t.id === theme) || THEMES[0];
    const currentAccent = ACCENTS.find(a => a.id === accent) || ACCENTS[0];

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
    }, [theme]);

    useEffect(() => {
        // 'default' = убрать атрибут, чтобы тема сама решила цвет accent'а.
        if (accent === 'default') {
            document.documentElement.removeAttribute('data-accent');
        } else {
            document.documentElement.setAttribute('data-accent', accent);
        }
        localStorage.setItem('accent', accent);
    }, [accent]);

    const setTheme = (id: ThemeId) => {
        if (THEMES.some(t => t.id === id)) {
            setThemeState(id);
        }
    };

    const setAccent = (id: AccentId) => {
        if (ACCENTS.some(a => a.id === id)) {
            setAccentState(id);
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
            cycleTheme,
            accent,
            accentName: currentAccent.name,
            setAccent,
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
