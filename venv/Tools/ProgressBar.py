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
from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QGridLayout
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QPushButton
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

        self.setWindowIcon(QIcon(os.getcwd()+'\\venv\Assets\Processing.png'))

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
    def __init__(self, title = 'Yes or no?'):
        super().__init__()
        self.title = title

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


## testing ##

class CustomDialog(QDialog):
    def __init__(self,parent=None):
        super().__init__(parent)

        self.setWindowTitle("HELLO!")
        self.a = 1

        QBtn = QDialogButtonBox.Ok | QDialogButtonBox.Cancel

        self.buttonBox = QDialogButtonBox(QBtn)
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        self.option_test = QCheckBox('A checkbox')
        self.option_test2 = QCheckBox('A checkbox2')

        self.layout = QVBoxLayout()
        self.layout.addWidget(self.option_test)
        self.layout.addWidget(self.option_test2)

        message = QLabel("Something happened, is that OK?")
        self.layout.addWidget(message)
        self.layout.addWidget(self.buttonBox)
        self.setLayout(self.layout)

    def getReturnSet(self):
        response = ''
        if self.option_test.isChecked():
            response = response+'Option 1'
        if self.option_test2.isChecked():
            response = response+'Option 2'
        return response



class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.a = 1

        self.setWindowTitle("My App")

        button = QPushButton("Press me for a dialog!")
        button.clicked.connect(self.button_clicked)
        self.setCentralWidget(button)

    def button_clicked(self):
        dlg = CustomDialog()
        dlg.exec()
        print(dlg.getReturnSet())

        print('this happened')


def test_frame():
    app = QApplication(sys.argv)
    ex = MainWindow()
    ex.show()
    app.exec_()

if __name__ == '__main__':
    test_frame()

## end ##