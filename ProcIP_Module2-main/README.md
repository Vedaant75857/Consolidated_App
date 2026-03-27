# Data Normalizer 

A self-contained, standalone single-sheet data processing and AI normalization tool.

---

## 🚀 How to Run the Application

Because this app is now detached from the large monorepo, you must run it in **two terminal windows**: one for the Python AI backend, and one for the React frontend.

**(Note: Our code was built into the `DataNormalizer` folder without the hyphen)**

### 1. Start the Python Backend
Open a new terminal (Command Prompt/PowerShell) and navigate to the backend folder:
```powershell
cd "c:\Shams\Test Cases for PROCIP\Module2\DataNormalizer\backend"
```

If you haven't installed dependencies yet, run:
```powershell
pip install -r requirements.txt
```

Start the Flask server:
```powershell
python app.py
```
*Your backend is now running on `http://localhost:5000` waiting for requests.*

---

### 2. Start the React Frontend
Open a *second* separate terminal and navigate to the frontend folder:
```powershell
cd "c:\Shams\Test Cases for PROCIP\Module2\DataNormalizer\frontend"
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
