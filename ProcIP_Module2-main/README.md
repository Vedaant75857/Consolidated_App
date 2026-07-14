# Data Normalizer 

A self-contained, standalone single-sheet data processing and AI normalization tool.

---

## 🚀 How to Run the Application

Within the consolidated suite, run this app in **two terminal windows**: one for the Python AI backend and one for the React frontend.

**(Note: Our code was built into the `DataNormalizer` folder without the hyphen)**

### 1. Start the Python Backend
Open a new terminal (Command Prompt/PowerShell) at the consolidated repository root:
```powershell
cd "C:\Users\75857\Downloads\Consolidated_App"
```

From the consolidated repository, install Python dependencies into the repo root `.venv`:
```powershell
.\setup.ps1
```

Start the Flask server:
```powershell
cd "C:\Users\75857\Downloads\Consolidated_App\ProcIP_Module2-main\backend"
..\..\.venv\Scripts\python.exe app.py
```
*Your backend is now running on `http://localhost:5000` waiting for requests.*

---

### 2. Start the React Frontend
Open a *second* separate terminal and navigate to the frontend folder:
```powershell
cd "C:\Users\75857\Downloads\Consolidated_App\ProcIP_Module2-main\frontend"
```

If you haven't installed Node dependencies yet, run:
```powershell
npm install
```

Start the Vite development server:
```powershell
npm run dev
```

*Your frontend will open automatically, or you can go to `http://localhost:5173` in your browser.*

### 🛠 How to Test the Flow
1. **Upload:** Drag and drop an Excel or CSV file. Enter your OpenAI API Key.
2. **Auto-Map:** Click to automatically align your file's columns with standard headers. Apply the changes.
3. **Normalize Dashboard:** Check off the modules you want to run (e.g., Dates, Regions, Currency). Click **"Run Pipeline"**.
4. **Download:** Export your cleaned dataset to Excel.
