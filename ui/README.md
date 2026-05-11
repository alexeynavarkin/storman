# storman-ui

React 19 + TypeScript + Tailwind CSS v4 + shadcn-style components + React
Router v7 + TanStack Query. Talks to the storman backend at `/api/*`.

## Dev mode (with HMR)

Two terminals:

```bash
# 1. Backend on :8443
storman serve --data-dir=/path/to/data-dir   # no --ui-dir

# 2. Frontend on :5173 with HMR + proxy
cd ui
npm install   # first time only
npm run dev
# open http://localhost:5173
```

Vite proxies `/api/*` to `http://localhost:8443` by default. Override with
`STORMAN_API=http://other-host:port npm run dev` if your backend listens
elsewhere.

The Vite proxy preserves cookies, so the session and CSRF tokens issued by
the backend work without CORS or origin gymnastics.

## Production build

```bash
cd ui
npm run build           # type-check + Vite production bundle → ui/dist/

storman serve --data-dir=... --ui-dir=/abs/path/to/ui/dist
```

The backend will serve hashed assets from `ui/dist/assets/*` and fall back to
`index.html` for any non-`/api` path so client-side routes (e.g. `/files/...`)
deep-link correctly.

## Project layout

```
ui/
├── package.json
├── vite.config.ts
├── tsconfig.json + tsconfig.app.json + tsconfig.node.json
├── index.html
└── src/
    ├── main.tsx                 # entry — providers + router
    ├── app/
    │   ├── providers.tsx        # QueryClient, Toaster
    │   └── root-layout.tsx      # <Routes>
    ├── routes/
    │   ├── login.tsx
    │   └── files.tsx            # file browser
    ├── components/
    │   ├── ui/                  # shadcn-style primitives (own, edit freely)
    │   ├── layout/              # header
    │   └── files/               # breadcrumb, file row, upload zone, dialogs
    ├── features/auth/           # current-user query, require-auth wrapper
    ├── lib/                     # api client, csrf, format, permissions, cn
    ├── types/                   # API DTO types (mirror of Go DTOs)
    └── styles/globals.css       # Tailwind v4 + tokens
```

## Code style

- Strict TS (`noUncheckedIndexedAccess`, `noImplicitOverride`).
- File names kebab-case, components PascalCase.
- `useState` for local state; TanStack Query for everything server-side.
- shadcn-style primitives live in `components/ui/` — own and edit them.
- Aesthetic: dark-first, sharp, minimal — `rounded-md`/`rounded-lg`, `h-9`,
  `text-sm`, density like Linear/Vercel. See `references/visual-style.md` in
  the `skill-frontend` skill for the full guide.
