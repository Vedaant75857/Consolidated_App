# Data Normalizer

The Data Normalizer is Module 2 of the consolidated ProcIP suite. From the
repository root, install dependencies with:

```powershell
.\setup.ps1
```

Run its backend and frontend in separate terminals:

```powershell
npm run dev:normalizer-be
npm run dev:normalizer-fe
```

The backend listens on `http://localhost:5000`, and Vite serves the frontend on
`http://localhost:3003`.

The normal workflow is to upload an Excel or CSV file, map its columns, select
normalization steps, run the pipeline, and export the cleaned dataset.
