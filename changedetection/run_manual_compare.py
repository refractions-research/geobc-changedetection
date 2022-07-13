# -------------------------------------------------------------------------------
# 
# This script provides a UI where users can select two files & fields to compare,
# then runs the comparison and writes the results to an output file
#
# Author:      Emily Gouge (Refractions Research)
# Created:     July 13, 2022
# Copyright:   (c) GeoBC
# -------------------------------------------------------------------------------
import tkinter as tk
import tkinter.filedialog as tkfd
import tkinter.ttk as ttk
import tkinter.messagebox as messagebox

import tempfile
import os
import sqlite3
from multiprocessing import Process, Queue
import logging

from core import utils
from core import change_detector

ERROR = 1
OK = 2

_logger = logging.getLogger()

#-------------------------------------------------------------------------------
# setup logging
#-------------------------------------------------------------------------------
def configure_logging():
    _logger.setLevel(logging.DEBUG)
    # console handler - info messages only
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    consolehandler.setFormatter(formatter)
    _logger.addHandler(consolehandler)

    # file handler - all messages
    fileloghandler = logging.FileHandler(os.path.join(utils.log_folder, "Change_Detection_Manual_Compare_" + utils.rundatetime + ".txt"), mode='a', encoding="utf-8",)
    fileloghandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileloghandler.setFormatter(formatter)
    _logger.addHandler(fileloghandler)


# Queue must be global
# for mulit-processing feedback 
_thread_queue = Queue()

#-------------------------------------------------------------------------------
# Wizard with two pages, the first
# for selecting input and output files, the second
# for selecting fields to process
#-------------------------------------------------------------------------------
class Wizard(tk.Frame):
    
    #initialize wizard 
    def __init__(self, parent):
        super().__init__(parent, bd=1, relief="raised")
        
        self.button_frame = tk.Frame(self, relief="flat")
        self.content_frame = tk.Frame(self, bd=4, relief="flat")
        
        sep = ttk.Separator(self.button_frame,orient='horizontal')
        sep.pack(fill='x')
        
        options = {"side":"right", "padx":"5", "ipadx":"5", "pady":"5"}

        self.cancel_button = tk.Button(self.button_frame, text="Cancel", command=self.cancel)
        self.next_button = tk.Button(self.button_frame, text="Next >>", command=self.next)
        self.back_button = tk.Button(self.button_frame, text="<< Back", command=self.back)       
        self.finish_button = tk.Button(self.button_frame, text="Finish", command=self.finish)
        self.finish_button.pack(**options)
        self.next_button.pack(**options)
        self.back_button.pack(**options)
        self.cancel_button.pack(**options)

        self.current_step = None
        
        self.step1 = Step1(self.content_frame)
        self.step2 =  Step2(self.content_frame);
        self.steps = [self.step1, self.step2]
        
        self.button_frame.pack(side="bottom", fill="x")
        self.content_frame.pack(side="top", fill="both", expand=True)

        
        self.show_step(0)
        
        self.pack(side="top", fill="both", expand=True)

    #cancel wizard
    def cancel(self):
        self.master.destroy()

    #move back
    def back(self):
        tmpstep = self.current_step 
        tmpstep = tmpstep - 1
        if (tmpstep < 0):
            tmpstep = 0
        self.show_step(tmpstep)
    
    #move next
    def next(self):
        tmpstep = self.current_step 
        tmpstep = tmpstep + 1
        if (tmpstep >= len(self.steps)):
            tmpstep = len(self.steps) - 1
        self.show_step(tmpstep)
    
    #finish
    def finish(self):
        if (len(self.step2.get_selected()) == 0):
            messagebox.showinfo("Fields", "You must selected at least one field")
            return
        
        file1 = self.step1.input1_txt.get().strip()
        layer1 = self.step1.layer1_cmb.get().strip()
        file2 = self.step1.input2_txt.get().strip()
        layer2 = self.step1.layer2_cmb.get().strip()
        fields = self.step2.get_selected()
        output_file = self.step1.output_txt.get().strip()
        
        #disable all buttons
        self.back_button["state"] = tk.DISABLED
        self.finish_button["state"] = tk.DISABLED
        self.next_button["state"] = tk.DISABLED
        self.cancel_button["state"] = tk.DISABLED
            
        #start processing in another thread
        self.p1 = Process(target=do_work, args=(_thread_queue, file1, layer1, file2, layer2, fields, output_file))
        self.p1.start()
        self.after(1000, self.poll_process)
    
    #poll sub process until complete
    def poll_process(self):
        if (self.p1.is_alive()):
            self.after(1000, self.poll_process)
            return
        else:    
            if (_thread_queue.get(0) == OK):
                messagebox.showinfo("Complete.", f"Comparison complete.\n\n{_thread_queue.get(1)}")
                self.show_step(0)
            else: 
                messagebox.showerror("Error Comparing Dataset", f"An error occurred while comparing datasets. {_thread_queue.get(1)}.  See log files for more details.")
                self.back_button["state"] = tk.NORMAL
                self.finish_button["state"] = tk.NORMAL
            
            self.cancel_button["state"] = tk.NORMAL


    #show specific wizard page
    #step = 1 shows first page, step = 2 shows the second page                
    def show_step(self, step):
        if step == 1:
            if self.step1.output_txt.get().strip() == "":
                messagebox.showinfo("Error", "Output file must be selected")
                return
            #displaying field selector
            sharedfields = self.get_fields()
            if (sharedfields is None):
                return 
            if (len(sharedfields) == 0):
                messagebox.showinfo("Fields", "These two datasets don't contain any shared fields")
                return
            
            self.step2.initfields(sharedfields)
            
        if self.current_step is not None:
            # remove current step
            current_step = self.steps[self.current_step]
            current_step.pack_forget()

        self.current_step = step

        new_step = self.steps[step]
        new_step.pack(side="top", fill="both", expand=True)
        
        if step == 0:
            # first step
            self.back_button["state"] = tk.DISABLED
            self.finish_button["state"] = tk.DISABLED
            self.next_button["state"] = tk.NORMAL

        elif step == len(self.steps)-1:
            # last step
            self.back_button["state"] = tk.NORMAL
            self.finish_button["state"] = tk.NORMAL
            self.next_button["state"] = tk.DISABLED

        else:
            # all other steps
            self.back_button["state"] = tk.NORMAL
            self.finish_button["state"] = tk.DISABLED
            self.next_button["state"] = tk.NORMAL

        
    #get shared fiels between two input files
    def get_fields(self):
        
        #read input files and get fields
        file1 = self.step1.input1_txt.get().strip()
        file2 = self.step1.input1_txt.get().strip()
        
        ds1 = utils.find_data_source(os.path.abspath(file1))
        if (ds1 is None):
            messagebox.showerror("File Error", "Could not read file " + file1 + " with ORG");
            return None
        
        ds2 = utils.find_data_source(os.path.abspath(file2))
        if (ds2 is None):
            messagebox.showerror("File Error", "Could not read file " + file2 + " with ORG");
            return None
    
        #layer names
        layer1 = ds1.GetLayer(self.step1.layer1_cmb.get().strip())
        layer2 = ds2.GetLayer(self.step1.layer2_cmb.get().strip())
        
        if (layer1 is None or layer2 is None):
            messagebox.showerror("Layer Error", "Error reading layers from data sources")
            return None
        
        fields1 = set()
        fields2 = set()
        
        layer1def = layer1.GetLayerDefn()
        for i in range(1,layer1def.GetFieldCount()):
            fields1.add(layer1def.GetFieldDefn(i).GetName())
            
        layer2def = layer2.GetLayerDefn()
        for i in range(1,layer2def.GetFieldCount()):
            fields2.add(layer2def.GetFieldDefn(i).GetName())
        
        intersection = fields1.intersection(fields2)
        
        return intersection

#-------------------------------------------------------------------------------
#Page 1 in the wizard
#This page collects two input files and one output files. 
#also contains ability to select layer; if input file contains
#more than one layer         
#-------------------------------------------------------------------------------
class Step1(tk.Frame):
    
    def __init__(self, parent):
        super().__init__(parent)

        header = tk.Label(self, text="Select input and output files.")
        sep = ttk.Separator(self, orient='horizontal')
                
        input_frame = tk.Frame(self)
        
        input1_lbl = tk.Label(input_frame, text="File 1:")
        self.input1_txt = tk.Entry(input_frame)
        input1_btn = tk.Button(input_frame, text = "file...", command=lambda: self.select_file('Select input file', self.input1_txt, self.layer1_cmb, self.layer1_lbl))
        input1dir_btn = tk.Button(input_frame, text = "dir...", command=lambda: self.select_dir('Select input directory', self.input1_txt, self.layer1_cmb, self.layer1_lbl))
        self.input1_txt.bind("<FocusOut>", lambda event:self.update_layers(self.input1_txt, self.layer1_cmb, self.layer1_lbl))
        
        self.layer1_lbl = tk.Label(input_frame, text="Layer:")
        self.layer1_cmb = ttk.Combobox(input_frame)
        
        self.layer1_lbl["state"] = tk.DISABLED
        self.layer1_cmb["state"] = tk.DISABLED
        
        input2_lbl = tk.Label(input_frame, text="File 2:")
        self.input2_txt = tk.Entry(input_frame)
        input2_btn = tk.Button(input_frame, text = "file...", command=lambda: self.select_file('Select input file', self.input2_txt, self.layer2_cmb, self.layer2_lbl))
        input2dir_btn = tk.Button(input_frame, text = "dir...", command=lambda: self.select_dir('Select input directory', self.input2_txt, self.layer2_cmb, self.layer2_lbl))
        self.input2_txt.bind("<FocusOut>", lambda event:self.update_layers(self.input2_txt, self.layer2_cmb, self.layer2_lbl))
        
        self.layer2_lbl = tk.Label(input_frame, text="Layer:")
        self.layer2_cmb = ttk.Combobox(input_frame)
        
        self.layer2_lbl["state"] = tk.DISABLED
        self.layer2_cmb["state"] = tk.DISABLED
        
        output_lbl = tk.Label(input_frame, text="Output File:")
        self.output_txt = tk.Entry(input_frame)
        output_btn = tk.Button(input_frame, text = "...", command=lambda: self.select_save_file('Select output file', self.output_txt, (('geopackage', '*.gpkg'),('All Files', '*.*'))))
        
        input1_lbl.grid(row = 0, column=0, padx=5, pady=5, sticky=tk.E)
        self.input1_txt.grid(row = 0, column=1, padx=5, pady=5, sticky=tk.EW, columnspan=2)
        input1_btn.grid(row = 0, column=3, padx=2, sticky=tk.E)
        input1dir_btn.grid(row = 0, column=4, padx=2, sticky=tk.E)
        
        self.layer1_lbl.grid(row = 1, column = 1, padx=5, pady=0, sticky=tk.W)
        self.layer1_cmb.grid(row = 1, column = 2, padx=5, pady=0, sticky=tk.EW )
        
        input2_lbl.grid(row = 2, column=0, padx=5, pady=5, sticky=tk.E)
        self.input2_txt.grid(row = 2, column=1, columnspan=2, padx=5, pady=5, sticky=tk.EW)  
        input2_btn.grid(row = 2, column=3, padx=2, sticky=tk.E)
        input2dir_btn.grid(row = 2, column=4, padx=2, sticky=tk.E)
        
        self.layer2_lbl.grid(row = 3, column = 1, padx=5, pady=0, sticky=tk.W)
        self.layer2_cmb.grid(row = 3, column = 2, padx=5, pady=0, sticky=tk.EW )
        
        output_lbl.grid(row = 4, column=0, padx=5, pady=5, sticky=tk.E)
        self.output_txt.grid(row = 4, column=1, columnspan=2, padx=5, pady=5, sticky=tk.EW)  
        output_btn.grid(row = 4, column=3, padx=5, sticky=tk.W)
        
        header.pack(side="top", fill="x", pady=10)
        sep.pack(fill='x')  
        input_frame.pack(side="top", fill="both")
        input_frame.columnconfigure(2, weight=1)

    #updates the layer combo with the file referenced in the text box         
    def update_layers(self, text, layercombo, layerlbl):
        filename = text.get().strip()
        
        if (filename == ""):
            return 
        
        layernames = get_layers(filename);
        if (layernames is None):
            layercombo['values'] = list()
            layercombo["state"] = tk.DISABLED
            layerlbl["state"] = tk.DISABLED
        else:
            layercombo['values'] = list(layernames)
            layercombo.current(0)
            if (len(layernames) == 1):
                layercombo["state"] = tk.DISABLED
                layerlbl["state"] = tk.DISABLED
            else:
                layercombo["state"] = tk.NORMAL
                layerlbl["state"] = tk.NORMAL
    
    #selects a file and updates the text box
    def select_file(self, title, toupdate, layercombo, layerlbl, filetypes = (('All Files (*.*)', '*'),)):
        filename = tkfd.askopenfilename(title = title, filetypes = filetypes)
        
        if (filename == ""):
            return
        toupdate.delete(0, tk.END)
        toupdate.insert(0, filename)
        toupdate.xview(tk.END)
        
        self.update_layers(toupdate,layercombo, layerlbl)
    
    #selects a directory and updates the text box
    def select_dir(self, title, toupdate, layercombo, layerlbl):
        filename = tkfd.askdirectory(title = title)
        
        if (filename == ""):
            return
        toupdate.delete(0, tk.END)
        toupdate.insert(0, filename)
        toupdate.xview(tk.END)
        
        self.update_layers(toupdate,layercombo, layerlbl)
    
    #selects the output file
    def select_save_file(self, title, toupdate, filetypes = (('All Files (*.*)', '*.*'),)):
        filename = tkfd.asksaveasfilename(title = title, filetypes = filetypes, defaultextension=".gpkg")
        if (filename == ""):
            return
        toupdate.delete(0, tk.END)
        toupdate.insert(0, filename)
        toupdate.xview(tk.END)
        
#-------------------------------------------------------------------------------        
#Page 2 in the wizard
#This page collects fields to use in comparison 
#-------------------------------------------------------------------------------
class Step2(tk.Frame):
    
    def __init__(self, parent):
        super().__init__(parent)

        header = tk.Label(self, text="Select fields to compare.")
        header.pack(side=tk.TOP, fill=tk.X, pady=10)
        
        sep = ttk.Separator(self, orient='horizontal')
        sep.pack(fill=tk.X)
        
        self.checkbox_widgets = []
        self.container = tk.Frame(self, bd=0, relief="solid", background="white")
        self.canvas = tk.Canvas(self.container, bd=0, relief="raised", background="white")
        self.scrollbar = tk.Scrollbar(self.container, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = tk.Frame(self.canvas, bd=0, relief="solid", background="white")
        

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(
                scrollregion=self.canvas.bbox("all")
            )
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
       
        self.container.bind('<Enter>', self._bound_to_mousewheel)
        self.container.bind('<Leave>', self._unbound_to_mousewheel)
        
    def _bound_to_mousewheel(self, event):
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _unbound_to_mousewheel(self, event):
        self.canvas.unbind_all("<MouseWheel>")

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
    
    def initfields(self, sharedfields):
        
        for w in self.checkbox_widgets:
            w.destroy()
                
        self.checkboxvalue_list = []
        self.sharedfields = list(sharedfields)
        self.sharedfields.sort()
        self.checkbox_widgets = []
        
        for i in range(0, len(sharedfields)-1):
            self.checkboxvalue_list.append(tk.IntVar(value=0))
            l = tk.Checkbutton(self.scrollable_frame, variable=self.checkboxvalue_list[i], text=self.sharedfields[i], anchor=tk.W, background="white", relief=tk.FLAT, highlightthickness=0)
            l.pack(side=tk.TOP, anchor = tk.W, fill=tk.X)
            self.checkbox_widgets.append(l)
            
        
        self.container.pack(side="left", fill="both", expand=True)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

    def get_selected(self):
        selected=[]
        for i in range(0, len(self.checkboxvalue_list)):
            if (self.checkboxvalue_list[i].get()):
                selected.append(self.sharedfields[i])
        
        return selected


#-------------------------------------------------------------------------------
#finds all spatial layers in the given file
#-------------------------------------------------------------------------------
def get_layers(filename):
    layers = utils.get_layers(filename)
    if (layers is None):
        messagebox.showerror("File Error", f"Could not read file {filename} with ORG");
        return None;
    return layers
                
#-------------------------------------------------------------------------------                
#does the manual file comparison   
#-------------------------------------------------------------------------------
def do_work(_thread_queue, file1, layer1, file2, layer2, fields, output_file):
    
    #initialize these values for this thread
    utils.parse_config()
    change_detector.configure_logging()
    
    #create a temporary file for the database
    dbtemp = tempfile.NamedTemporaryFile(delete=False)
    try:
        db_connection = sqlite3.connect(dbtemp.name)
        try:
            
            providerstats={}
            
            ds1_table = "dataset1"
            ds2_table = "dataset2"
            changetable = "changes"
                
            change_detector.load_data_and_compute_hash(db_connection, ds1_table, file1, layer1, None, fields, [])
            change_detector.load_data_and_compute_hash(db_connection, ds2_table, file2, layer2, None, fields, [])
    
            duplicate_features_1 = change_detector.find_duplicate_features(db_connection, ds1_table)
            duplicate_features_2 = change_detector.find_duplicate_features(db_connection, ds2_table)
            
            providerstats[utils.DataStatistic.NUM_OLD_DUPLICATE_RECORDS] = len(duplicate_features_1[0])
            providerstats[utils.DataStatistic.OLD_DUPLICATE_RECORDS] = duplicate_features_1[1]
            
            providerstats[utils.DataStatistic.NUM_NEW_DUPLICATE_RECORDS] = len(duplicate_features_2[0])
            providerstats[utils.DataStatistic.NEW_DUPLICATE_RECORDS] = duplicate_features_2[1]
            
            changetable = "changes"
            _logger.debug("running cd")
            change_detector.create_and_populate_change_table(db_connection, ds2_table, "ds2", ds1_table, "ds1", changetable, fields, [])
            change_detector.export_change_table(changetable, db_connection, output_file)
            
            change_detector.compute_stats(db_connection, ds2_table, ds1_table, changetable, providerstats)
            
            msg = f"""Output File: {output_file}
            {utils.format_statistics(providerstats)}
            """  
            
            #capture some stats and display it to the user
            _thread_queue.put(OK)
            _thread_queue.put(msg)
        except Exception as ex:
            _thread_queue.put(ERROR)
            _thread_queue.put(ex)
        finally:
            db_connection.close()
    finally:
        dbtemp.close()
        os.unlink(dbtemp.name)
        assert not os.path.exists(dbtemp.name)
            
#-------------------------------------------------------------------------------            
#function to run that runs the gui
#-------------------------------------------------------------------------------
def gui_main():
    window = tk.Tk()
    window.title("Manual Change Detection")
    Wizard(window)
    window.geometry("500x400")
    window.mainloop()

#-------------------------------------------------------------------------------
#function to run if command arguments provided
#-------------------------------------------------------------------------------
def command_main():
    file1 = utils.args.args[0]
    file2 = utils.args.args[1]
    output_file = utils.args.args[2]
    fields = utils.args.args[3].split(",")
        
    if (file1 == None or file1.strip() == ""):
        print("Invalid file1")
        return
    if (not os.path.exists(file1)):
        print("File1 not found")
        return
    if (file2 == None or file2.strip() == ""):
        print("Invalid file2")
        return
    if (not os.path.exists(file2)):
        print("File2 not found")
        return    
    if (output_file == None or output_file.strip() == ""):
        print("Invalid output file")
        return
    if (fields == None or len(fields) == 0 ):
        print("Invalid fields - at least one field must be specified")
        return

    _logger.info("Comparing files")
    _logger.info(f"Input 1: {file1}")
    _logger.info(f"Input 2: {file2}")
    _logger.info(f"Output: {output_file}")
    _logger.info(f"Fields: {fields}")
    
    do_work(_thread_queue, file1, None, file2, None, fields, output_file)
    
    if (_thread_queue.get(0) == OK):
        print("Complete:")
    else:
        print("Error:")
    print(_thread_queue.get(1))

#-------------------------------------------------------------------------------
# Main function
#-------------------------------------------------------------------------------        
if __name__ == '__main__':
    utils.parse_config()
    configure_logging()
    
    if (len(utils.args.args) == 0):
        gui_main()
    else:
        command_main()
        
