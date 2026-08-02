# Data Normalizer

The Data Normalizer is Module 2 of the consolidated ProcIP suite. From the
repository root, install dependencies with:

```powershell
.\setup.ps1
```

Start the unified backend and this module's frontend in separate terminals:

```powershell
npm run dev:backend
npm run dev:normalizer-fe
```

The unified backend listens on `http://localhost:8000`, and Vite serves the
frontend on `http://localhost:3003`. Vite rewrites this frontend's `/api/*`
requests to the Module 2 namespace on the unified backend.

The normal workflow is to upload an Excel or CSV file, map its columns, select
normalization steps, run the pipeline, and export the cleaned dataset.
