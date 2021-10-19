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
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QFileDialog
from PyQt5.QtWidgets import QGridLayout
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QProgressBar
from PyQt5.QtWidgets import QDialogButtonBox




class ProgressBarWindow(QWidget):
    def __init__(self,process_name='a process'):
        super().__init__()
        self.__size__ = 100
        # creating progress bar
        self.layout = QGridLayout()
        self.pbar = QProgressBar(self)
        self.layout.addWidget(self.pbar,1,0,1,3)
        self.fy_icon = '\\venv\Assets\Processing.png'

        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

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
    def __init__(self, title = 'Yes or no?', icon='duck'):
        super().__init__()
        self.title = title

        if icon == 'duck':
            self.fy_icon = '\\venv\Assets\Duckworth2.png'
        elif icon == 'append-a':
            self.fy_icon = '\\venv\Assets\Duckworth2.png'
        elif icon == 'append-b':
            self.fy_icon = '\\venv\Assets\Duckworth2.png'
        elif icon == 'merge':
            self.fy_icon = '\\venv\Assets\Duckworth2.png'

        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.left = 100
        self.top = 300
        self.width = 400
        self.height = 200

    def initUI(self, message_txt = 'Yes/no text', message_q = 'A question?'):
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

    def __init__(self,list_items,instruction='Select at least 1 from the following:'):
        super().__init__()
        self.title = '...'

        self.fy_icon = '\\venv\Assets\Duckworth2.png'
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
    def __init__(self,lst_input_reqs, parent=None):
        super().__init__(parent)
        self.return_textbox = []
        self.lst_output_req = {}

        self.fy_icon = '\\venv\Assets\Duckworth2.png'
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.setWindowTitle("Please enter text.")
        self.setGeometry(100, 300, 0, 0)
        self.layout = QGridLayout()

        # button to show the above entered text
        self.accept_button = QPushButton('Send data', self)
        # connect button to function on_click
        self.accept_button.clicked.connect(self.on_click)
        self.layout.addWidget(self.accept_button, 0, 1)

        # button to show the above entered text
        self.best_guess_button = QPushButton('See best matches', self)
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

    def __init__(self, dialog_type='file_dialog', name = 'Please select a file'):
        super().__init__()
        self.fy_icon = '\\venv\Assets\Duckworth2.png'
        self.setWindowIcon(QIcon(os.getcwd()+self.fy_icon))

        self.left = 10
        self.top = 10
        self.width = 640
        self.height = 480

        self.out_file_name = ''
        self.name = name
        self.initUI(dialog_type)

    def initUI(self, dialog_type):
        self.setGeometry(self.left, self.top, self.width, self.height)

        if dialog_type == 'file_dialog':
            self.openFileNameDialog()

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
        dlg = FileDialogObject('some words')
        print('d')
        file_name = dlg.get_file_name()
        print('e')

        print(file_name)

        print('this happened')


def test_frame():
    app = QApplication(sys.argv)
    ex = MainWindow()
    ex.show()
    print('a')
    app.exec_()
    print('z')

if __name__ == '__main__':
    test_frame()

## end ##