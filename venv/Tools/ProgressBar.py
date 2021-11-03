# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20210609
# CreateFor: Franklin Young International

import os
import sys
import time
import datetime

from PyQt5 import QtCore
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QFrame
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QComboBox
from PyQt5.QtWidgets import QCompleter
from PyQt5.QtWidgets import QFileDialog
from PyQt5.QtWidgets import QGridLayout
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QProgressBar
from PyQt5.QtWidgets import QDialogButtonBox


def set_icon(icon):
    if icon == 'duck':
        return '\\venv\Assets\Duckworth2.png'
    elif icon == 'progress':
        return '\\venv\Assets\Processing.png'
    elif icon == 'append-a':
        return '\\venv\Assets\LeftAppendFile.png'
    elif icon == 'append-b':
        return '\\venv\Assets\RightAppendFile.png'
    elif icon == 'merge':
        return '\\venv\Assets\MergeFiles.png'
    elif icon == 'split':
        return '\\venv\Assets\SplitterIcon.png'


class ProgressBarWindow(QWidget):
    def __init__(self,process_name='a process',icon='progress'):
        super().__init__()
        self.__size__ = 100
        # creating progress bar
        self.layout = QGridLayout()
        self.pbar = QProgressBar(self)
        self.layout.addWidget(self.pbar,1,0,1,3)

        self.set_icon = set_icon
        self.update_icon(icon)

        self.duration_label = QLabel('')
        self.duration_label.setText('Duration unknown.')

        self.layout.addWidget(self.duration_label,0,0)
        # add a label here for elapsed time and estimated completion

        self.remaining_label = QLabel('')
        self.remaining_label.setAlignment(QtCore.Qt.AlignRight)
        self.remaining_label.setText('This might take a while.')

        self.layout.addWidget(self.remaining_label,0,2)
        # add a label here for elapsed time and estimated completion

        self.count_label = QLabel('')
        self.count_label.setAlignment(QtCore.Qt.AlignCenter)
        self.count_label.setText('Please wait')

        self.layout.addWidget(self.count_label,0,1)
        # add a label here for elapsed time and estimated completion

        self.setGeometry(100, 300, 400, 70)
        self.setWindowTitle(process_name)
        self.setLayout(self.layout)

    def update_icon(self, icon):
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

    def set_anew(self,total):
        self.start_time = datetime.datetime.now()
        self.run_length=total
        self.count = 0

    def update_unknown(self):
        QApplication.processEvents()

    def update_bar(self,step=0):
        if step == 0:
            count = 0
            QApplication.processEvents()
        else:
            count = int(self.__size__*(step/self.run_length))

            self.percent_done = round(step/self.run_length,5)
            if self.percent_done == 0:
                self.percent_done = 0.00001

            self.elapsed = datetime.datetime.now() - self.start_time

            self.est_total_time = self.elapsed/self.percent_done
            self.estimated_completion = self.est_total_time-self.elapsed

            self.elapsed = str(self.elapsed).split('.')[0]
            self.estimated_completion = str(self.estimated_completion).split('.')[0]

            self.duration_label.setText('Duration: ' + self.elapsed)
            self.count_label.setText(str(step)+"/"+str(self.run_length))
            self.remaining_label.setText('Est. Remaining: ' + self.estimated_completion)

            if self.count != count:
                self.count = count
                self.pbar.setValue(self.count)
            QApplication.processEvents()

            if (step == self.run_length):
                time.sleep(1)
                self.close()


class YesNoDialog(QWidget):
    def __init__(self, title = 'Yes or no?'):
        super().__init__()
        self.title = title
        self.set_icon = set_icon

        self.left = 100
        self.top = 300
        self.width = 400
        self.height = 200

    def initUI(self, message_txt = 'Yes/no text', message_q = 'A question?', icon='progress'):
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))
        self.yes_selected = False
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

        buttonReply = QMessageBox.question(self, message_txt, message_q,
                                           QMessageBox.Yes | QMessageBox.No)
        if buttonReply == QMessageBox.Yes:
            self.yes_selected = True


class JoinSelectionDialog(QDialog):
    # this dialog will be provided when joining two files
    # this will assign the join type and the join headers
    # nothing fancy

    def __init__(self,list_items,instruction='Select at least 1 from the following:', icon = 'duck'):
        super().__init__()
        self.title = '...'

        self.set_icon = set_icon
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.list_items = list_items
        self.left = 100
        self.top = 300
        self.width = 200 + 50*round(len(list_items)/20)
        if len(list_items) < 21:
            self.height = 90 + (18*len(list_items))
        else:
            self.height = 90 + (18 * 20)

        self.item_pos = 2
        self.item_pos_col = 0
        self.return_list = []
        self.list_items.sort()
        self.canceled = False

        self.layout = QGridLayout()
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

        self.dialog_label = QLabel('')
        self.dialog_label.setText(instruction)
        self.layout.addWidget(self.dialog_label,0,0)

        self.generate_buttons()

        QBtn = QDialogButtonBox.Ok | QDialogButtonBox.Cancel

        self.buttonBox = QDialogButtonBox(QBtn)
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        if self.item_pos_col != 0:
            self.layout.addWidget(self.buttonBox, 20, self.item_pos_col)
        else:
            self.layout.addWidget(self.buttonBox, self.item_pos, self.item_pos_col)
        self.setLayout(self.layout)


    def generate_buttons(self):
        self.option_list = {}
        for each_item in self.list_items:
            if each_item not in self.option_list:
                self.option_list[each_item] = QCheckBox(each_item)

                self.layout.addWidget(self.option_list[each_item], self.item_pos, self.item_pos_col,1,1)
                self.item_pos += 1
                if self.item_pos > 20:
                    self.item_pos = 2
                    self.item_pos_col+=1

    def get_selected_items(self):
        self.return_list = []
        for each_item in self.option_list:
            if self.option_list[each_item].isChecked():
                self.return_list.append(self.option_list[each_item].text())

        return self.return_list


class TextBoxObject(QDialog):
    def __init__(self,lst_input_reqs, parent=None, icon = 'duck'):
        super().__init__(parent)
        self.return_textbox = []
        self.lst_output_req = {}

        self.set_icon = set_icon
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.setWindowTitle("Please enter text.")
        self.setGeometry(100, 300, 0, 0)
        self.layout = QGridLayout()

        # button to show the above entered text
        self.accept_button = QPushButton('Send data', self)
        self.accept_button.setToolTip('This will send the data to the database.')
        # connect button to function on_click
        self.accept_button.clicked.connect(self.on_click)
        self.layout.addWidget(self.accept_button, 0, 1)

        # button to show the above entered text
        self.best_guess_button = QPushButton('See best matches', self)
        self.best_guess_button.setToolTip('This doesn\'t do anything yet.')
        # connect button to function on_click
        self.best_guess_button.clicked.connect(self.best_guesses)
        self.layout.addWidget(self.best_guess_button, 0, 2)

        self.addInit(lst_input_reqs)

        self.setLayout(self.layout)
        self.show()

    def addInit(self,lst_input_reqs):
        pos = 1
        for each_input_req in lst_input_reqs:
            #self.layout.addWidget(self.base_data_button, 1, column_pos)
            req_label = QLabel(each_input_req[0])
            req_label.setToolTip(each_input_req[2])
            self.layout.addWidget(req_label, pos, 0)

            # edit text box
            text_test = QLineEdit(self)
            text_test.setMaxLength(each_input_req[1])
            text_test.setText(each_input_req[3])
            if len(each_input_req) == 5:
                text_test.setPlaceholderText(each_input_req[4])
            self.layout.addWidget(text_test, pos, 1, 1, 2)

            self.return_textbox.append([each_input_req[0], text_test])

            pos += 1


    def on_click(self):
        for each_text_entry in self.return_textbox:
            self.lst_output_req[each_text_entry[0]] = each_text_entry[1].text()

        self.accept()

    def best_guesses(self):
        print('This does nothing right now')

    def getReturnSet(self):
        return self.lst_output_req


class FileDialogObject(QWidget):

    def __init__(self, dialog_type='file_dialog', name = 'Please select a file', icon='duck'):
        super().__init__()
        self.left = 10
        self.top = 10
        self.width = 640
        self.height = 480

        self.set_icon = set_icon
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.out_file_name = ''
        self.name = name
        self.initUI(dialog_type)

    def initUI(self, dialog_type):
        self.setGeometry(self.left, self.top, self.width, self.height)

        if dialog_type == 'file_dialog':
            self.openFileNameDialog()

        elif dialog_type == 'files_dialog':
            self.openFileNamesDialog()

        elif dialog_type == 'save_dialog':
            self.saveFileDialog()

        self.show()

    # this is what we use to find a file
    def openFileNameDialog(self):
        files_to_get = 'Excel Files (*.xlsx);;Text files (*.txt)'

        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        self.out_file_name, _ = QFileDialog.getOpenFileName(self, self.name, '',
                                                   files_to_get, options=options)

    # this is what we use to find a file
    def openFileNamesDialog(self):
        files_to_get = 'Excel Files (*.xlsx);;Text files (*.txt)'

        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        self.out_file_names, _ = QFileDialog.getOpenFileNames(self, self.name, '',
                                                   files_to_get, options=options)

    # this probably won't be used
    # however, user might want to name their outputs in some cases
    def saveFileDialog(self):
        files_to_get = 'Excel Files (*.xlsx);;Text files (*.txt)'

        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        self.out_file_name, _ = QFileDialog.getSaveFileName(self, self.name, '',
                                                   files_to_get, options=options)

    def get_file_name(self):
        return self.out_file_name

    def get_file_names(self):
        return self.out_file_names


class AssignCategoryDialog(QDialog):
    def __init__(self, new_description, lst_categories, parent=None, icon = 'duck'):
        super().__init__(parent)
        self.return_textbox = []
        self.lst_output_req = {}
        self.set_category_dict(lst_categories)

        self.set_icon = set_icon
        self.fy_icon = self.set_icon(icon)
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.setWindowTitle("Please pick a good category.")
        self.setGeometry(100, 300, 500, 100)

        self.layout = QGridLayout()

        # top level: description
        desc_info = 'Description: '+ new_description
        self.desc_label = QLabel(desc_info)
        self.desc_label.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        self.desc_label.setWordWrap(True)
        self.desc_style = 'QLabel{border: 1px solid grey; color: black; background-color: white}'
        self.frame_style = 'QFrame{background-color: 1px solid grey}'
        self.desc_label.setStyleSheet(self.desc_style)
        self.layout.addWidget(self.desc_label, 0, 0, 2, 3)

        self.frame_line_1 = QFrame()
        self.frame_line_1.setFrameShape(QFrame.HLine)
        self.frame_line_1.setStyleSheet(self.frame_style)
        self.layout.addWidget(self.frame_line_1, 2, 0, 1, 4)

        # second left-most catgegory label
        self.category1_label = QLabel('Category level 1: ')
        self.layout.addWidget(self.category1_label, 3, 0, 1, 1)

        # second right big drop down
        self.category1_dropdown = QComboBox()
        self.category1_dropdown.currentIndexChanged.connect(self.update_second_level)
        self.layout.addWidget(self.category1_dropdown, 3, 1, 1, 2)

        # second left-most catgegory label
        self.category2_label = QLabel('Category level 2: ')
        self.layout.addWidget(self.category2_label, 4, 0, 1, 1)

        # second right big drop down
        self.category2_dropdown = QComboBox()
        self.category2_dropdown.currentIndexChanged.connect(self.update_third_level)
        self.layout.addWidget(self.category2_dropdown, 4, 1, 1, 2)

        # second left-most catgegory label
        self.category3_label = QLabel('Category level 3: ')
        self.layout.addWidget(self.category3_label, 5, 0, 1, 1)

        # second right big drop down
        self.category3_dropdown = QComboBox()
        self.layout.addWidget(self.category3_dropdown, 5, 1, 1, 2)

        # attempt to accept the data
        self.assign_button = QPushButton('\nAssign by level\n', self)
        self.assign_button.setToolTip('This will assign the category based on level selection to the product.')
        self.assign_button.clicked.connect(self.on_assign)
        self.layout.addWidget(self.assign_button, 3, 3, 3, 1)

        self.frame_line_2 = QFrame()
        self.frame_line_2.setFrameShape(QFrame.HLine)
        self.frame_line_2.setStyleSheet(self.frame_style)
        self.layout.addWidget(self.frame_line_2, 6, 0, 1, 4)

        # second left-most catgegory label
        self.full_category_label = QLabel('Full Category: ')
        self.layout.addWidget(self.full_category_label, 7, 0, 1, 1)

        # second right big drop down
        self.full_category_dropdown = QComboBox()
        self.full_category_dropdown.addItems(lst_categories)
        self.layout.addWidget(self.full_category_dropdown, 7, 1, 1, 2)

        # attempt to accept the data
        self.take_full_button = QPushButton('Assign category', self)
        self.take_full_button.setToolTip('This will assign the category from the full category to the product.')
        self.take_full_button.clicked.connect(self.on_take_full)
        self.layout.addWidget(self.take_full_button, 7, 3, 1, 1)


        self.frame_line_3 = QFrame()
        self.frame_line_3.setFrameShape(QFrame.HLine)
        self.frame_line_3.setStyleSheet(self.frame_style)
        self.layout.addWidget(self.frame_line_3, 8, 0, 1, 4)

        # second left-most catgegory label
        self.word1_label = QLabel('Word1: ')
        self.word1_label.setToolTip('first word in the pair to match with the category')
        self.layout.addWidget(self.word1_label, 9, 0, 1, 1)

        # edit text box
        self.word1_collect = QLineEdit(self)
        self.word1_collect.setText('')
        self.word1_collect.setPlaceholderText('required')
        self.word1_collect.setToolTip('Remember: garbage in, garbage out')
        self.layout.addWidget(self.word1_collect, 9, 1, 1, 2)

        # second left-most catgegory label
        self.word2_label = QLabel('Word2: ')
        self.word2_label.setToolTip('second word in the pair to match with the category')
        self.layout.addWidget(self.word2_label, 10, 0, 1, 1)

        # edit text box
        self.word2_collect = QLineEdit(self)
        self.word2_collect.setText('')
        self.word2_collect.setPlaceholderText('required')
        self.word2_collect.setToolTip('Remember: garbage in, garbage out')
        self.layout.addWidget(self.word2_collect, 10, 1, 1, 2)

        # skip this one
        self.skip_button = QPushButton('\nskip assignment\n', self)
        self.skip_button.setToolTip('Pass on assigning')
        self.skip_button.clicked.connect(self.on_skip)
        self.layout.addWidget(self.skip_button, 0, 3, 2, 1)

        self.setLayout(self.layout)

        self.set_dropdowns()

        self.show()

    def set_category_dict(self, categories):
        self.dct_categories = {}
        for each_cat in categories:
            split_cat = each_cat.split('/')
            if len(split_cat) == 4:
                if split_cat[1] not in self.dct_categories:
                    self.dct_categories[split_cat[1]] = {split_cat[2]:[split_cat[3]]}
                elif split_cat[2] not in self.dct_categories[split_cat[1]]:
                    self.dct_categories[split_cat[1]][split_cat[2]] = [split_cat[3]]
                else:
                    self.dct_categories[split_cat[1]][split_cat[2]].append(split_cat[3])

            elif len(split_cat) == 3:
                if split_cat[1] not in self.dct_categories:
                    self.dct_categories[split_cat[1]] = {split_cat[2]:[]}
                elif split_cat[2] not in self.dct_categories[split_cat[1]]:
                    self.dct_categories[split_cat[1]][split_cat[2]] = []

            elif len(split_cat) == 2:
                if split_cat[1] not in self.dct_categories:
                    self.dct_categories[split_cat[1]] = {}


    def set_dropdowns(self):
        top_level = list(self.dct_categories.keys())
        top_level.sort()
        self.category1_dropdown.addItems(top_level)
        completer1 = QCompleter(top_level)
        self.category1_dropdown.setCompleter(completer1)

        mid_level = list(self.dct_categories[top_level[0]].keys())
        mid_level.sort()
        self.category2_dropdown.addItems(mid_level)
        completer2 = QCompleter(mid_level)
        self.category2_dropdown.setCompleter(completer2)

        bot_level = self.dct_categories[top_level[0]][mid_level[0]]
        bot_level.sort()
        self.category3_dropdown.addItems(bot_level)
        completer3 = QCompleter(bot_level)
        self.category3_dropdown.setCompleter(completer3)


    def update_second_level(self):
        self.category2_dropdown.clear()
        mid_level = list(self.dct_categories[str(self.category1_dropdown.currentText())].keys())
        mid_level.sort()
        if mid_level:
            self.category2_dropdown.addItems(mid_level)
            completer2 = QCompleter(mid_level)
            self.category2_dropdown.setCompleter(completer2)


    def update_third_level(self):
        self.category3_dropdown.clear()
        mid_name = str(self.category2_dropdown.currentText())
        if mid_name == '':
            mid_level = list(self.dct_categories[str(self.category1_dropdown.currentText())].keys())
            bot_level = self.dct_categories[str(self.category1_dropdown.currentText())][mid_level[0]]
        else:
            bot_level = self.dct_categories[str(self.category1_dropdown.currentText())][str(self.category2_dropdown.currentText())]

        if bot_level:
            bot_level.sort()
            self.category3_dropdown.addItems(bot_level)
            completer3 = QCompleter(bot_level)
            self.category3_dropdown.setCompleter(completer3)

    def on_take_full(self):
        self.lst_output_req['AssignedCategory'] = str(self.full_category_dropdown.currentText())
        self.lst_output_req['Word1'] = str(self.word1_collect.text())
        self.lst_output_req['Word2'] = str(self.word2_collect.text())

        self.accept()

    def on_assign(self):
        assigned_category = 'All Products/' + str(self.category1_dropdown.currentText())
        if str(self.category2_dropdown.currentText()) != '':
            assigned_category = assigned_category + '/' + str(self.category2_dropdown.currentText())

        if str(self.category3_dropdown.currentText()) != '':
            assigned_category = assigned_category + '/' + str(self.category3_dropdown.currentText())

        self.lst_output_req['AssignedCategory'] = assigned_category
        self.lst_output_req['Word1'] = str(self.word1_collect.text())
        self.lst_output_req['Word2'] = str(self.word2_collect.text())

        self.accept()

    def on_skip(self):
        self.reject()

    def getReturnSet(self):
        return self.lst_output_req


## testing ##



class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.a = 1

        self.setWindowTitle("My App")

        button = QPushButton("Press me for a dialog!")
        button.clicked.connect(self.button_clicked)
        self.setCentralWidget(button)

    def button_clicked(self):
        # the button is just a proxy for some other trigger

        print('b')
        categories = ['All Products/Chemicals/Analytical Chemicals/Acids','All Products/Chemicals/Analytical Chemicals/Bases','All Products/Chemicals/Analytical Chemicals/Buffers','All Products/Chemicals/Analytical Chemicals/Caustics','All Products/Chemicals/Analytical Chemicals/Specialty Chemicals','All Products/Chemicals/Analytical Chemicals/Toxic ','All Products/Chemicals/Biochemicals/Cell Culture Medium','All Products/Chemicals/Biochemicals/Cell Cultures','All Products/Chemicals/Biochemicals/Reagents and Supplements for Cell Culture','All Products/Chemicals/Filtration/Filter Aids','All Products/Chemicals/Inorganic Chemicals/Aluminium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Ammonium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Antimony Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Barium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Bismuth Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Boron Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Calcium Compounds','All Products/Chemicals/Inorganic Chemicals/Calcium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Cesium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Chlorine Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Chromium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Cobalt Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Compounds and Salts','All Products/Chemicals/Inorganic Chemicals/Copper Compounds','All Products/Chemicals/Inorganic Chemicals/Copper Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Gadolinium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Gold Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/High Purity Compounds and Salts','All Products/Chemicals/Inorganic Chemicals/Inorganic Salts','All Products/Chemicals/Inorganic Chemicals/Iodine Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Iron Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Lanthanum Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Lead Compounds','All Products/Chemicals/Inorganic Chemicals/Lithium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Magnesium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Manganese Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Mercury Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Nickel Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Osmium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Phosphorus Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Platinum Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Potassium Compounds','All Products/Chemicals/Inorganic Chemicals/Potassium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Ruthenium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Silicon Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Silver Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Sodium Compounds','All Products/Chemicals/Inorganic Chemicals/Sodium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Strontium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Sulfur Compounds','All Products/Chemicals/Inorganic Chemicals/Tin Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Titanium Inorganic Compounds','All Products/Chemicals/Inorganic Chemicals/Zinc Inorganic Compounds','All Products/Chemicals/Organic Chemicals/Acetylides','All Products/Chemicals/Organic Chemicals/Ammonium Compounds','All Products/Chemicals/Organic Chemicals/Benzenoids','All Products/Chemicals/Organic Chemicals/Esoteric Organic Compounds','All Products/Chemicals/Organic Chemicals/Formaldehyde and Paraformaldehyde','All Products/Chemicals/Organic Chemicals/Graphite','All Products/Chemicals/Organic Chemicals/Hydrocarbon derivatives','All Products/Chemicals/Organic Chemicals/Hydrocarbons','All Products/Chemicals/Organic Chemicals/Lignans Neolignans and Related Compounds','All Products/Chemicals/Organic Chemicals/Lipids and Lipid-like Molecules','All Products/Chemicals/Organic Chemicals/Magnesium Compounds','All Products/Chemicals/Organic Chemicals/Nucleosides Nucleotides and Analogues','All Products/Chemicals/Organic Chemicals/Organic Acids and Derivatives','All Products/Chemicals/Organic Chemicals/Organic cations','All Products/Chemicals/Organic Chemicals/Organic Nitrogen Compounds','All Products/Chemicals/Organic Chemicals/Organic Oxygen Compounds','All Products/Chemicals/Organic Chemicals/Organic Salts','All Products/Chemicals/Organic Chemicals/Organic zwitterions','All Products/Chemicals/Organic Chemicals/Organohalogen Compounds','All Products/Chemicals/Organic Chemicals/Organoheterocyclic Compounds','All Products/Chemicals/Organic Chemicals/Organophosphorus Compounds','All Products/Chemicals/Organic Chemicals/Organopnictogen compounds','All Products/Chemicals/Organic Chemicals/Organosulfur Compounds','All Products/Chemicals/Organic Chemicals/Phenylpropanoids and Polyketides','All Products/Chemicals/Organic Chemicals/Unclassified Organic Compounds','All Products/Chemicals/Organic Chemicals/Zinc Compounds','All Products/Chemicals/Other Chemicals/Amino Acids','All Products/Chemicals/Other Chemicals/Antibiotics','All Products/Chemicals/Other Chemicals/Decontaminants and Detergents','All Products/Chemicals/Other Chemicals/Desiccants','All Products/Chemicals/Other Chemicals/Isotopically Labeled Compound ','All Products/Chemicals/Other Chemicals/Metals and or Alloys','All Products/Chemicals/Other Chemicals/Metalurgical Assay','All Products/Chemicals/Other Chemicals/Other Custom Chemicals ','All Products/Chemicals/Other Chemicals/Polymers','All Products/Chemicals/Other Chemicals/Solid Phase Resins Supports and Sieves','All Products/Chemicals/Other Chemicals/Water','All Products/Chemicals/Reagents/Chromatography and Mass Spectrometry Reagents','All Products/Chemicals/Reagents/Histology Reagents','All Products/Chemicals/Reagents/Karl Fischer Reagents','All Products/Chemicals/Reagents/Kjeldahl Reagents','All Products/Chemicals/Reagents/Microbiology Reagents','All Products/Chemicals/Reagents/Miscellaneous Reagents for Molecular Biology','All Products/Chemicals/Reagents/Urine Reagent Ketones','All Products/Chemicals/Reagents/Urine Reagent Microalbumin','All Products/Chemicals/Solutions and Standard Chemicals/Solutions for Chemical Testing','All Products/Chemicals/Solutions and Standard Chemicals/Standards','All Products/Chemicals/Solutions and Standard Chemicals/Titration','All Products/Chemicals/Solvents/Acetone','All Products/Chemicals/Solvents/Acetonitrile','All Products/Chemicals/Solvents/Chloroform','All Products/Chemicals/Solvents/Denatured Alcohols','All Products/Chemicals/Solvents/Esoteric Organic Solvents','All Products/Chemicals/Solvents/Ethanol','All Products/Chemicals/Solvents/Ethyl Acetate','All Products/Chemicals/Solvents/Heptanes','All Products/Chemicals/Solvents/Hexanes','All Products/Chemicals/Solvents/High Purity','All Products/Chemicals/Solvents/Isopropanol IPA','All Products/Chemicals/Solvents/Methanol','All Products/Chemicals/Solvents/Methylene Chloride','All Products/Chemicals/Solvents/Pentanes','All Products/Chemicals/Solvents/Petroleum Ether','All Products/Chemicals/Solvents/Propanol','All Products/Chemicals/Solvents/Pyridine','All Products/Chemicals/Solvents/Solvent Blends','All Products/Chemicals/Solvents/Stoddard Solvent','All Products/Chemicals/Solvents/Tetrahydrofuran','All Products/Chemicals/Solvents/Toluene','All Products/Healthcare/Amplification ','All Products/Healthcare/Clinical Controls Calibrators and Standards','All Products/Healthcare/Clinical Diagnostic Kits/Drug Test Controls','All Products/Healthcare/Clinical Diagnostic Kits/Drug Test Cups','All Products/Healthcare/Clinical Diagnostic Kits/Drugs of Abuse','All Products/Healthcare/Clinical Diagnostic Kits/Fecal Occult Blood Test','All Products/Healthcare/Clinical Diagnostic Kits/Kit Supplies','All Products/Healthcare/Clinical Diagnostic Kits/Mono','All Products/Healthcare/Clinical Diagnostic Kits/Other Kits','All Products/Healthcare/Clinical Diagnostic Kits/Pregnancy','All Products/Healthcare/Clinical Diagnostic Kits/Strep A','All Products/Healthcare/Clinical Diagnostic Kits/Test Strips','All Products/Healthcare/Clinical Diagnostic Kits/Urine Cups','All Products/Healthcare/COVID 19/COVID Antibody','All Products/Healthcare/COVID 19/COVID Antigen','All Products/Healthcare/COVID 19/Covid Test kits','All Products/Healthcare/COVID 19/Viral Transport Media','All Products/Healthcare/DNA Synthesis ','All Products/Healthcare/Gastroenterology','All Products/Healthcare/Hematology','All Products/Healthcare/Immunohistochemistry','All Products/Healthcare/Molecular ','All Products/Healthcare/Stains and Dyes/Hematology Stains','All Products/Healthcare/Stains and Dyes/Histological and Cytological Stains','All Products/Healthcare/Stains and Dyes/Other Stains and Dyes','All Products/Life Science/Cellomics/Bioprocess Systems and Accessories','All Products/Life Science/Cellomics/Cancer Research','All Products/Life Science/Cellomics/Cell Based Assay','All Products/Life Science/Cellomics/Cell Lines','All Products/Life Science/Cellomics/Flow Cytometry','All Products/Life Science/Cellomics/Goods Buffers','All Products/Life Science/Cellomics/Growth Factor Products','All Products/Life Science/Cellomics/Sera','All Products/Life Science/Cellomics/Specialty Growth Systems','All Products/Life Science/Chromatography/Chromatography Columns','All Products/Life Science/Chromatography/Chromatography Equipment Accessories','All Products/Life Science/Chromatography/Chromatography Injection Products','All Products/Life Science/Chromatography/Chromatography Media','All Products/Life Science/Chromatography/Chromatography Supplies','All Products/Life Science/Chromatography/Chromatography Syringes','All Products/Life Science/Chromatography/Ion Chromatography (IC) Modules','All Products/Life Science/Clinical Instruments/Blood Analyzers','All Products/Life Science/Clinical Instruments/Clinical Chemistry Analyzers','All Products/Life Science/Clinical Instruments/Electrochemistry Analyzers and Systems','All Products/Life Science/Clinical Instruments/Electrophoresis','All Products/Life Science/Clinical Instruments/Immunoassay Systems','All Products/Life Science/Clinical Instruments/Microinjectors for Molecular Biology','All Products/Life Science/Clinical Instruments/Plasma Thawers','All Products/Life Science/Clinical Instruments/PTSD and Panic Attacks','All Products/Life Science/Clinical Instruments/Urine Analyzers','All Products/Life Science/Clinical Research/Microbiology Apparatus','All Products/Life Science/Clinical Research/Software License','All Products/Life Science/Clinical Supplies/Embedding','All Products/Life Science/Clinical Supplies/General Purpose Syringes','All Products/Life Science/Clinical Supplies/Other Products','All Products/Life Science/Clinical Supplies/Patient Care','All Products/Life Science/Clinical Supplies/Sharps Disposal','All Products/Life Science/Clinical Supplies/Specimen Collection','All Products/Life Science/Clinical Supplies/Syringes','All Products/Life Science/Consumables/Cartridges','All Products/Life Science/Consumables/Installation Kits','All Products/Life Science/Consumables/Modules and Deionizers','All Products/Life Science/Consumables/Other Accessories','All Products/Life Science/Consumables/Starter Kits','All Products/Life Science/Consumables/Support Manuals','All Products/Life Science/Consumables/UV Lamps','All Products/Life Science/Consumables/Wall Mounting Kits','All Products/Life Science/Genomics/DNA Sequencing Systems and Accessories','All Products/Life Science/Genomics/Enzymes','All Products/Life Science/Genomics/Nucleic Acid Research','All Products/Life Science/Histology/Histology and Cytology','All Products/Life Science/Histology/Histology Equipment','All Products/Life Science/Histology/Tissue Samplers','All Products/Life Science/Lab Consumables/Carts Educational','All Products/Life Science/Lab Consumables/Classroom Educational Kits','All Products/Life Science/Lab Consumables/Combustion','All Products/Life Science/Lab Consumables/Data Logging','All Products/Life Science/Lab Consumables/Dessicators','All Products/Life Science/Lab Consumables/Educational Apparatus','All Products/Life Science/Lab Consumables/Implements','All Products/Life Science/Lab Consumables/Laboratory Use Buckets and Pails','All Products/Life Science/Lab Consumables/Mortars and Pestles','All Products/Life Science/Lab Consumables/Preserved Specimens','All Products/Life Science/Lab Consumables/Special Material Nonvacuum Desiccators','All Products/Life Science/Lab Consumables/Sterilization Accessories','All Products/Life Science/Lab Consumables/Storage Transport and Temperature Maintenance','All Products/Life Science/Lab Consumables/Temperature','All Products/Life Science/Lab Consumables/Weights for Balances','All Products/Life Science/Lab Supplies/Aluminium','All Products/Life Science/Lab Supplies/Analyzers and Accessories','All Products/Life Science/Lab Supplies/Animal Research','All Products/Life Science/Lab Supplies/Applicators and Swabs','All Products/Life Science/Lab Supplies/Bags','All Products/Life Science/Lab Supplies/Bench Protectors','All Products/Life Science/Lab Supplies/Clamps','All Products/Life Science/Lab Supplies/Containers','All Products/Life Science/Lab Supplies/Dissection Equipment','All Products/Life Science/Lab Supplies/Film and Foil Wrapping and Dispensers','All Products/Life Science/Lab Supplies/Gauges','All Products/Life Science/Lab Supplies/Hand Care','All Products/Life Science/Lab Supplies/Lab Cleaning Supplies','All Products/Life Science/Lab Supplies/Lab Product Dispensers','All Products/Life Science/Lab Supplies/Lab Tools','All Products/Life Science/Lab Supplies/Light Sources','All Products/Life Science/Lab Supplies/Other Metals','All Products/Life Science/Lab Supplies/Racks and Supports','All Products/Life Science/Lab Supplies/Samplers','All Products/Life Science/Lab Supplies/Sieves','All Products/Life Science/Lab Supplies/Specimen Labeler Accessories','All Products/Life Science/Lab Supplies/Stainless Steel','All Products/Life Science/Lab Supplies/Stationary ','All Products/Life Science/Lab Supplies/Timers','All Products/Life Science/Lab Supplies/Other Lab Accessories','All Products/Life Science/Lab Supplies/Wire','All Products/Life Science/Microplate Instrumentation and Equipment','All Products/Life Science/Microplate Instrumentation and Equipment/Microarray Scanners','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Absorbance Readers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Accessories','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Fluorescence Readers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Instrumentation Software','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Multimode Readers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Readers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Sealing Equipment','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Single Mode Readers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Washer Accessories','All Products/Life Science/Microplate Instrumentation and Equipment/Microplate Washers','All Products/Life Science/Microplate Instrumentation and Equipment/Microplates','All Products/Life Science/Microplate Instrumentation and Equipment/Plastic Microplates','All Products/Life Science/Microplate Instrumentation and Equipment/Robotic Systems','All Products/Life Science/Microplate Instrumentation and Equipment/Validation Tools','All Products/Life Science/Proteomics/Antibodies','All Products/Life Science/Proteomics/Autoradiography Supplies','All Products/Life Science/Proteomics/Dialysis and Desalting Supplies','All Products/Life Science/Proteomics/Immunology','All Products/Life Science/Proteomics/Membranes for Hybridization and Transfer','All Products/Life Science/Proteomics/Protein Detection','All Products/Life Science/Proteomics/Protein Extraction and Purification','All Products/Life Science/Proteomics/Proteins','All Products/Life Science/Proteomics/Proteomic Research','All Products/Safety/Controlled Environment Modular Cleanroom','All Products/Safety/Controlled Environments Accessories','All Products/Safety/Controlled Environments Housekeeping','All Products/Safety/Facility Safety and Maintenance/Facility Maintenance','All Products/Safety/Facility Safety and Maintenance/Facility Safety']

        description = 'In this example we would be seeing a very long description that included useful keywords that generate a good response. Here\'s an example: 100mg vial of monoclonal mouse antibody IL23R. It turns out that wasn\'t all the long after all. so here it is again In this example we would be seeing a very long description that included useful keywords that generate a good response. Here\'s an example: 100mg vial of monoclonal mouse antibody IL23R. In this example we would be seeing a very long description that included useful keywords that generate a good response. Here\'s an example: 100mg vial of monoclonal mouse antibody IL23R.'
        description2 = 'a short one'
        dlg = AssignCategoryDialog(description, categories)
        print('d')
        dlg.exec_()
        print('e')
        result_set = dlg.getReturnSet()
        print(result_set)

        print('this happened')


def test_frame():

    sys.excepthook = excepthook
    #custom_font = QFont("Avant Garde", 12)

    #QApplication.setFont(custom_font)
    app = QApplication(sys.argv)

    ex = MainWindow()
    ex.show()
    print('a')
    ret = app.exec_()
    print('z')
    sys.exit(ret)


def excepthook(exc_type, exc_value, exc_tb):
    tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    crash_path = os.getcwd() + '\\venv\Assets\CrashReport.txt'
    error_path = tb.split('\n')
    error_call = error_path[-2]

    print(tb)
    QApplication.quit()


if __name__ == '__main__':
    test_frame()

## end ##