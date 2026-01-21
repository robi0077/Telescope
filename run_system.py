
import subprocess
import time
import os
import sys

def run_system():
    # 1. Start Backend
    print("Starting Telescope Backend (FastAPI)...")
    backend_env = os.environ.copy()
    backend_env["PYTHONPATH"] = "src"
    
    # Using the venv python
    python_exe = os.path.join("venv", "Scripts", "python.exe")
    if not os.path.exists(python_exe):
        print("Venv not found, using system python")
        python_exe = sys.executable

    # Use shell=True to keep window open if it crashes immediately? 
    # Better: we pipe output to our main window so we see it.
    backend = subprocess.Popen(
        [python_exe, "src/telescope/server.py"],
        cwd=os.getcwd(),
        env=backend_env,
        # stdout=subprocess.PIPE, 
        # stderr=subprocess.PIPE 
        # Letting it inherit stdout/stderr so user sees logs directly in main window
    )
    
    time.sleep(3) # Give it a moment to crash if it's going to crash
    
    if backend.poll() is not None:
        print("\n\nCRITICAL ERROR: Backend failed to start!")
        print("Please check the error messages above.")
        input("Press Enter to exit...")
        sys.exit(1)
    
    # 2. Start Frontend
    print("Starting Telescope Frontend (Vite)...")
    frontend_dir = os.path.join(os.getcwd(), "frontend")
    
    frontend = subprocess.Popen(
        "npm run dev", 
        cwd=frontend_dir, 
        shell=True
    )
    
    print("\n------------------------------------------------")
    print("System is running!")
    print("Backend: http://localhost:8000/docs")
    print("Frontend: http://localhost:5173")
    print("------------------------------------------------\n")
    
    try:
        while True:
            time.sleep(1)
            if backend.poll() is not None:
                print("Backend process died unexpectedly!")
                break
            if frontend.poll() is not None:
                print("Frontend process died unexpectedly!")
                break
    except KeyboardInterrupt:
        print("\nShutting down...")
        backend.terminate()
        frontend.terminate()

if __name__ == "__main__":
    run_system()
