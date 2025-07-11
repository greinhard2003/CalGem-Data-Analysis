import tkinter as tk
from tkinter import filedialog, messagebox

from GenerateCSV import *

#Creates a minimally functional application to provide an interface to access the GenerateFiles function
#Very little UI
class Application:
    def __init__(self, root):
        self.root = root
        self.root.title = "TEMP"
        self.production_file = None
        self.wells_file = None

        tk.Label(root, text="1. Select Oil and Gas Production File").pack()
        tk.Button(root, text= "Choose CSV File", command=self.select_production_file).pack()
        self.prod_label = tk.Label(root, text="No file selected", fg="gray")
        self.prod_label.pack(pady=(0, 15), padx=(10, 10))

        tk.Label(root, text="2. Select Wells Data File").pack()
        tk.Button(root, text= "Choose CSV File", command=self.select_wells_file).pack()
        self.wells_label = tk.Label(root, text="No file selected", fg="gray")
        self.wells_label.pack(pady=(0, 15), padx=(10, 10))

        self.run_btn = tk.Button(root, text="Run and Generate Output", command=self.run_pipeline, state=tk.DISABLED)
        self.run_btn.pack(pady=15)

        self.status_label = tk.Label(root, text="", fg="green")
        self.status_label.pack()

    def select_production_file(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.production_file = path
            self.prod_label.config(text=os.path.basename(path), fg="black")
            self.check_ready()

    def select_wells_file(self):
        path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if path:
            self.wells_file = path
            self.wells_label.config(text=os.path.basename(path), fg="black")
            self.check_ready()

    def check_ready(self):
        if self.production_file and self.wells_file:
            self.run_btn.config(state=tk.NORMAL)

    def run_pipeline(self):
        try:
            GenerateFiles(self.production_file, self.wells_file)
            self.status_label.config(text="Successfully created dataset.csv and summary.csv", fg="green")
        except Exception as e:
            messagebox.showerror("Error", f"Something went wrong:\n{str(e)}")
            self.status_label.config(text="Failed to generate output", fg="red")

if __name__ == '__main__':
    root = tk.Tk()
    app = Application(root)
    root.mainloop()