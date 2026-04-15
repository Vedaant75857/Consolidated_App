DataScopingTool
================

Getting started
---------------
1. Double-click DataScopingTool.exe
2. Wait for the browser to open (this may take up to 60 seconds)
3. Do NOT close the black console window — it powers the app
4. To stop the app: close the console window

Troubleshooting
---------------
- If the app won't start, look in the "logs" folder (next to the EXE)
  and send the latest .log file to your support contact.

- For a quick health check, open a command prompt in this folder and run:
      DataScopingTool.exe --diagnostics

- If a specific module is not loading, check the console window for
  error messages about which service failed.

- Make sure no other programs are using ports 3000, 3001, 5000, or 3005.
  The app will try nearby ports automatically if these are busy.
