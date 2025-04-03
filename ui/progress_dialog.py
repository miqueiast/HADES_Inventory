#progress_dialog.py
import tkinter as tk
from tkinter import ttk

class ProgressDialog(tk.Toplevel):
    def __init__(self, parent, title="Processando", message="Por favor aguarde..."):
        super().__init__(parent)
        self.title(title)
        self.geometry("400x150")
        self.resizable(False, False)
        
        ttk.Label(self, text=message).pack(pady=10)
        
        self.progress = ttk.Progressbar(
            self, 
            orient="horizontal", 
            length=300, 
            mode="indeterminate"
        )
        self.progress.pack(pady=10)
        
        self.btn_cancel = ttk.Button(self, text="Cancelar", command=self.cancel)
        self.btn_cancel.pack(pady=5)
        
        self.cancelled = False
        
    def show(self):
        self.progress.start()
        self.grab_set()
        self.wait_visibility()
        self.transient(self.master)
        
    def cancel(self):
        self.cancelled = True
        self.close()
        
    def close(self):
        self.progress.stop()
        self.grab_release()
        self.destroy()