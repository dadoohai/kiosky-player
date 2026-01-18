# Repository Guidelines

## Project Structure & Module Organization

- The repository currently contains no tracked source files or directories. Treat this as a greenfield project and keep new work organized from the start.
- Suggested baseline layout (adjust to the stack you choose):
  - `src/` for application code
  - `tests/` or `__tests__/` for automated tests
  - `assets/` for static files (images, fonts, etc.)
  - `scripts/` for developer utilities
  - `docs/` for architecture notes and design decisions

## Build, Test, and Development Commands

- No build/test tooling is present yet. When you introduce a toolchain, document the exact commands here.
- Example command formats to standardize on (replace with real commands once added):
  - `npm run dev` — start the local dev server
  - `npm test` — run the full test suite
  - `npm run build` — produce a production build

## Coding Style & Naming Conventions

- No formatter or linter configuration is present. Keep code consistent within each file and align with the language’s standard style guide.
- Recommended defaults until tooling is added:
  - Indentation: 2 spaces for web/JS/TS, 4 spaces for Python.
  - Naming: `camelCase` for variables/functions, `PascalCase` for types/classes, `kebab-case` for filenames when applicable.
- If you add formatters (e.g., Prettier, Black, gofmt), include the config files and update this section.

## Testing Guidelines

- No testing framework is configured yet. Add tests alongside new features once a test runner is chosen.
- Suggested conventions once a framework is added:
  - Test files named `*.test.*` or `*_test.*`
  - Arrange/Act/Assert structure within test cases

## Commit & Pull Request Guidelines

- Commit history is not yet available. Use clear, imperative messages (e.g., “Add initial API skeleton”).
- Pull requests should include:
  - A concise summary of changes
  - Steps to verify (commands or manual checks)
  - Screenshots or recordings for UI changes

## Security & Configuration Tips

- Store secrets in environment variables or a local `.env` file (never commit secrets).
- If you introduce configuration files, document required keys and provide a `.env.example`.
