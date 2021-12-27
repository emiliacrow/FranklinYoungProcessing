# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20210608
# CreateFor: Franklin Young International

import os
import glob
import pandas
import datetime
import threading
import warnings

from Tools.ProgressBar import FileDialogObject
from Tools.ProgressBar import ProgressBarWindow

class FileFinder():
    def __init__(self,):
        # will have to make this a configuration
        self.selected_file = None

    # akin to openFileNameDialog
    def ident_file(self,window_title='Please select a file',path = ''):

        obFileDialog = FileDialogObject('file_dialog', window_title)
        file_name = obFileDialog.get_file_name()
        if len(file_name) > 0:

            self.selected_file = file_name
            self.file_base_location = self.selected_file.rpartition('/')[0]

            self.selected_file = self.selected_file.replace('/','\\\\')

            self.process_time = str(datetime.datetime.now())
            self.process_time = self.process_time.replace(' ','_')
            self.process_time = self.process_time.partition('.')[0]
            self.process_time = self.process_time.replace(':','-')

            self.basic_out_file_path = self.selected_file.replace('.xlsx','_prcd_'+self.process_time+'.xlsx')
            return True, self.basic_out_file_path
        else:
            return False, 'No file selected'

    def ident_files(self, window_title='Please select a file',path = ''):

        obFileDialog = FileDialogObject('files_dialog', window_title)
        file_names = obFileDialog.get_file_names()
        file_set = []

        if len(file_names) > 1:
            for each_path in file_names:
                selected_file = each_path
                file_base_location = selected_file.rpartition('/')[0]

                selected_file = selected_file.replace('/','\\\\')
                file_set.append(selected_file)


            process_time = str(datetime.datetime.now())
            process_time = process_time.replace(' ','_')
            process_time = process_time.partition('.')[0]
            process_time = process_time.replace(':','-')

            self.basic_out_file_path = selected_file.replace('.xlsx','_prcd_'+process_time+'.xlsx')
            return True, file_set
        else:
            return False, 'No file selected'

    # this will be depricated
    # it's only used for images, and that's a stupid way to do it
    def ident_directory(self, window_title = 'Please select files', path = None):
        obFileDialog = FileDialogObject('files_dialog', window_title)
        file_names = obFileDialog.get_file_names()
        file_set = []

        if len(file_names) > 1:
            for each_path in file_names:
                selected_file = each_path
                file_base_location = selected_file.rpartition('/')[0]

                selected_file = selected_file.replace('/','\\\\')
                file_set.append(selected_file)

            return True, file_set
        else:
            return False, 'No file selected'

        root = tk.Tk()
        root.withdraw()
        # select directory
        directory = filedialog.askdirectory(initialdir=path, title=window_title,mustexist=True)

        directory = directory+'/*.*'
        lst_images = glob.glob(directory)

        clean_image_paths = []

        for each_image_path in lst_images:
            clean_image_path = each_image_path.replace('\\','\\\\')
            clean_image_path = clean_image_path.replace('/','\\\\')

            clean_image_name = clean_image_path.rpartition('\\')[2]

            clean_image_paths.append([clean_image_path,clean_image_name])

        return clean_image_paths


    def read_xlsx(self, base_data_file=''):
        if base_data_file != '':
            self.selected_file = base_data_file
        self.obProgressBar = ProgressBarWindow('Reading file...')
        self.obProgressBar.set_anew(10)
        self.obProgressBar.show()
        self.obProgressBar.update_unknown()

        # alternately add sheet_name=0,1,2,3 etc. for the tab to use
        # this makes sure we capture any 'Na' values rather than translating to nan

        excel_dataframe = pandas.read_excel(self.selected_file,keep_default_na=False, na_values=['_'],dtype=str)

        self.obProgressBar.close()

        return excel_dataframe


    # save dialog? no
    def write_xlsx(self,df_product,bumper,include_date = True, pb_desc = 'Creating file...'):

        self.obProgressBar = ProgressBarWindow(pb_desc)
        self.obProgressBar.set_anew(10)
        self.obProgressBar.show()
        self.obProgressBar.update_unknown()

        if  include_date:
            self.out_file_path = self.basic_out_file_path.replace('_prcd_','_'+bumper+'_prcd_')
        else:
            split_name = self.basic_out_file_path.partition('_prcd_')
            self.out_file_path = split_name[0]+'_prcd_.xlsx'
            self.out_file_path = self.out_file_path.replace('_prcd_', '_' + bumper)

        with pandas.ExcelWriter(self.out_file_path, mode='w') as writer:
            df_product.to_excel(writer,index=False)

        self.obProgressBar.close()

    def write_xlsx_sub_folder(self,df_product,bumper,include_date = False, pb_desc = 'Creating file...'):
        self.obProgressBar = ProgressBarWindow(pb_desc,icon='split')
        self.obProgressBar.set_anew(10)
        self.obProgressBar.show()
        self.obProgressBar.update_unknown()

        if include_date:
            self.out_file_path = self.basic_out_file_path.replace('_prcd_', '_' + bumper + '_prcd_')
        else:
            split_name = self.basic_out_file_path.partition('_prcd_')
            self.out_file_path = split_name[0] + '_prcd_.xlsx'
            self.out_file_path = self.out_file_path.replace('_prcd_', '_' + bumper)

            file_directory, whack, filename = self.out_file_path.rpartition('\\\\')

            bumper = bumper.rpartition('_')[0]

            self.out_file_path = file_directory+'\\\\'+bumper
            if not os.path.exists(self.out_file_path):
                os.makedirs(self.out_file_path)
            self.out_file_path = self.out_file_path +'\\\\' + filename

        with pandas.ExcelWriter(self.out_file_path, mode='w') as writer:
            df_product.to_excel(writer, index=False)

        self.obProgressBar.close()


def process_file(obFileFinder, file_action_selected):
    # this should all be broken out into a different file
    b_file_found, selected_file = obFileFinder.ident_file()
    if b_file_found:
        df_excel = obFileFinder.read_xlsx(selected_file)
        out_df_excel = df_excel.copy()
        lst_fish_vwr = []
        lst_vwr_thom = []
        lst_thom_fish = []

        return_count = 0

        for col_name, row in df_excel.iterrows():
            return_count += 1
            fisher_name = str(row['FisherManufacturerName'])
            vwr_name = str(row['VWRManufacturerName'])
            thomas_name = str(row['ThomasManufacturerName'])

            fish_vwr_val = fuzz.token_sort_ratio(fisher_name.lower(),vwr_name.lower())
            lst_fish_vwr.append(fish_vwr_val)
            vwr_thom_val = fuzz.token_sort_ratio(vwr_name.lower(),thomas_name.lower())
            lst_vwr_thom.append(vwr_thom_val)
            thom_fish_val = fuzz.token_sort_ratio(thomas_name.lower(),fisher_name.lower())
            lst_thom_fish.append(thom_fish_val)

        out_df_excel['vwr_thom'] = lst_vwr_thom
        out_df_excel['fish_vwr'] = lst_fish_vwr
        out_df_excel['thom_fish'] = lst_thom_fish

        out_df_excel.to_excel(selected_file.replace('.xlsx','_out.xlsx'))

        out_string = 'Processed {} lines of data.'.format(return_count)
    else:
        out_string = 'No File Selected'


class BaseDataLoader():
    def __init__(self):
        self.name = 'Lou Gehrig'

    def set_file_finder(self, obFileFinder):
        self.obFileFinder = obFileFinder

    def set_ingester(self, obIngester):
        self.obIngester = obIngester



class myThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        run_test_frame
        print ("Starting " + self.name)
        print_time(self.name, 5, self.counter)
        print ("Exiting " + self.name)

def print_time(threadName, counter, delay):
    while counter:
        if exitFlag:
            threadName.exit()
        time.sleep(delay)
        print ("{}: {}".format(threadName, time.ctime(time.time())))
        counter -= 1

def testing_threads():
    # Create new threads
    thread1 = myThread(1, "Thread-1", 1)
    thread2 = myThread(2, "Thread-2", 2)

    # Start new Threads
    thread1.start()
    thread2.start()




# for testing
if __name__ == '__main__':
    print('entered at toolbox')
    run_test_frame()