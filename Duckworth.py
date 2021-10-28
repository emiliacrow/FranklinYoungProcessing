# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20210813
# CreateFor: Franklin Young International

import os
import sys
import time
import traceback

from PyQt5.QtGui import QIcon
from PyQt5.QtGui import QFont
from PyQt5.QtCore import QTimer

from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QComboBox
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import QGridLayout
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QRadioButton
from PyQt5.QtWidgets import QApplication


from Tools.Pathway import Pathways


def main():
    feel = 'I feel the cosmos.'
    sys.excepthook = excepthook
    custom_font = QFont("Avant Garde", 12)

    QApplication.setFont(custom_font)
    app = QApplication(sys.argv)
    # make and display the gui
    obDuckworth = DuckworthWindow()
    obDuckworth.show()
    # catch errors
    ret = app.exec_()
    sys.exit(ret)


def excepthook(exc_type, exc_value, exc_tb):
    tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    crash_path = os.getcwd() + '\\venv\Assets\CrashReport.txt'
    error_path = tb.split('\n')
    error_call = error_path[-2]

    with open(crash_path, 'w') as f:
        f.write(':::CRASH REPORT:::\n')
        f.write(tb)

    alert = QMessageBox()
    alert.setIcon(QMessageBox.Critical)
    alert.setWindowTitle('A Crash Occurred!')

    error_msg = 'Error Message:\n' + (error_call.partition('\"')[2]).partition('\"')[0]
    alert.setText(error_msg)

    full_alert_text = 'Full message:\n' + error_call
    alert.setDetailedText(full_alert_text)
    x = alert.exec_()

    QApplication.quit()


class DuckworthWindow(QWidget):
    def __init__(self, parent=None):
        # super means it runs the base class __init__ first
        super(DuckworthWindow, self).__init__(parent)

        self.setWindowIcon(QIcon(os.getcwd() + '\\venv\Assets\Duckworth2.png'))
        self.base_interval = 5000

        self.update_timer = QTimer()
        self.update_timer.setInterval(self.base_interval)
        self.update_timer.setSingleShot(False)
        self.update_timer.timeout.connect(self.message_scroll)
        self.update_timer.start(self.base_interval)

        self.success_string_style1 = 'QLabel{border: 2px solid black; color: black; background-color: white}'
        self.success_string_style2 = 'QLabel{border: 2px solid black; color: black; background-color: lightgrey}'
        self.active_string_style = 'QLabel{border: 2px solid black; color: yellow; background-color: darkgrey}'
        self.failure_string_style = 'QLabel{border: 2px solid darkred; color: darkred; background-color: darkgrey}'

        self.message_style = self.success_string_style1
        self.message_scroll_text = [['','Please make a selection',self.success_string_style1]]
        self.message_number = 0

        self.setWindowTitle('Duckworth, at your service. (Sequoia\'s butler)')
        self.setGeometry(100, 100, 0, 0)
        self.layout = QGridLayout()

        self.is_testing_buttons()
        self.is_testing = False

        self.file_process_buttons(0)
        self.base_data_buttons(1)
        self.ingestion_buttons(2)
        self.update_data_buttons(3)
        self.contract_buttons(4)

        self.setLayout(self.layout)
        self.obPathway = Pathways()

    def message_scroll(self):
        if len(self.message_scroll_text) > 0:
            if self.message_number == 0:
                display_text = ':: ' + self.message_scroll_text[self.message_number][1] + ' ::'
                self.update_timer.setInterval(4000)
            else:
                display_text = '[run:'+str(self.message_number) + ']'+self.message_scroll_text[self.message_number][0] + ' : ' + self.message_scroll_text[self.message_number][1]
                self.update_timer.setInterval(self.base_interval)

            result_style = self.message_scroll_text[self.message_number][2]

            if result_style == self.failure_string_style:
                self.message_style = self.failure_string_style

            elif self.message_style == self.success_string_style1:
                self.message_style = self.success_string_style2
            else:
                self.message_style = self.success_string_style1

            self.action_label.setStyleSheet(self.message_style)

            temp_message = ''
            temp_len = 0
            while temp_len < len(display_text):
                time.sleep(.009)
                temp_len += 1
                temp_message = display_text[:temp_len]
                self.action_label.setText(temp_message)
                QApplication.processEvents()

            if self.message_number + 1 == len(self.message_scroll_text):
                self.message_number = 0
            else:
                self.message_number += 1

    def set_new_tooltip(self):
        lst_tip_message = []
        run_count = 0
        for each_message in self.message_scroll_text:
            result_message = '[task:'+str(run_count)+']'+each_message[1]
            lst_tip_message.append(result_message)
            run_count += 1

        tip_message_set = str(lst_tip_message)
        tip_message_set = tip_message_set.replace("', '","<br>")
        tip_message_set = tip_message_set.replace("['","")
        tip_message_set = tip_message_set.replace("']","")

        self.action_label.setToolTip(tip_message_set)


    def is_testing_buttons(self):
        # all the stuffing for the is testing value
        self.testing_button = QRadioButton('Live')
        self.testing_button.setStyleSheet('QRadioButton{border: 1px solid black; background-color: pink}')
        self.testing_button.setChecked(False)
        self.testing_button.toggled.connect(lambda: self.testing_button_action(self.testing_button))

        self.testing_button.setToolTip('Defines which environment to use.')

        self.layout.addWidget(self.testing_button, 0, 0)

        self.action_label = QLabel('')
        self.message_scroll()
        self.layout.addWidget(self.action_label, 0, 1, 1, 3)

        self.hard_exit_button = QPushButton('Quit')
        self.hard_exit_button.setStyleSheet('QPushButton{background-color: pink}')
        self.hard_exit_button.setToolTip('Attempts to quit the process')

        self.hard_exit_button.clicked.connect(exit)
        self.layout.addWidget(self.hard_exit_button, 0, 4)

    def testing_button_action(self, button):
        if button.isChecked():
            self.testing_button.setText('Staging')
            self.testing_button.setStyleSheet('QRadioButton{border: 1px solid black; background-color: lightblue}')
            self.is_testing = True
        else:
            self.testing_button.setText('Live')
            self.testing_button.setStyleSheet('QRadioButton{border: 1px solid black; background-color: pink}')
            self.is_testing = False


    def file_process_buttons(self, column_pos):
        # all the stuffing for file processing actions
        self.all_file_action_options = ['Assign FyPartNumbers', 'File Merger Tool', 'Category Training',
                                    'Category Assignment', 'Category Picker', 'Extract Attributes',
                                    'Unicode Correction', 'Generate Upload File', 'File Splitter Tool',
                                    'Load Image Files', 'Product Action Review']

        self.file_action_options = ['Assign FyPartNumbers', 'File Merger Tool', 'Category Picker', 'Category Training',
                                    'Category Assignment', 'Extract Attributes',
                                    'Unicode Correction', 'Generate Upload File', 'File Splitter Tool',
                                    'Product Action Review']

        self.file_action_options.sort()

        self.process_file_button = QPushButton('Process a file')
        self.process_file_button.setStyleSheet('QPushButton{background-color: lightgreen}')

        self.process_file_button.setToolTip('For processing files.')

        self.process_file_button.clicked.connect(self.file_process_kickoff)
        self.layout.addWidget(self.process_file_button, 1, column_pos)

        self.file_action_dropdown = QComboBox()
        self.file_action_dropdown.addItems(self.file_action_options)
        self.layout.addWidget(self.file_action_dropdown, 2, column_pos)

    def file_process_kickoff(self):
        file_action_selected = str(self.file_action_dropdown.currentText())
        self.action_label.setText('Processing: ' + file_action_selected)
        self.action_label.setStyleSheet(self.active_string_style)
        QApplication.processEvents()

        success, message = self.obPathway.file_processing_pathway(self.is_testing, file_action_selected)

        if success:
            self.message_scroll_text.append([file_action_selected,message,self.success_string_style1])

        else:
            self.message_scroll_text.append([file_action_selected,message,self.failure_string_style])
        self.message_number = self.message_scroll_text.index(self.message_scroll_text[-1])
        self.set_new_tooltip()

        self.completion_alert(file_action_selected, message)


    def base_data_buttons(self,column_pos):
        self.base_data_tables = ['Category', 'Manufacturer', 'Vendor', 'Country', 'UNSPSC Codes', 'FSC Codes', 'Hazardous Code', 'NAICS Code', 'Unit of Issue-Symbol']
        self.base_data_tables.sort()

        self.base_data_button = QPushButton('Load Base Data')
        self.base_data_button.setStyleSheet('QPushButton{background-color: lightgreen}')

        self.base_data_button.setToolTip('Provides manual entry of data into the database.')

        self.base_data_button.clicked.connect(self.base_data_kickoff)
        self.layout.addWidget(self.base_data_button, 1, column_pos)

        self.basedata_dropdown = QComboBox()
        self.basedata_dropdown.addItems(self.base_data_tables)
        self.layout.addWidget(self.basedata_dropdown, 2, column_pos)

    def base_data_kickoff(self):
        table_selected = str(self.basedata_dropdown.currentText())
        self.action_label.setText('Processing {} data.'.format(table_selected))
        self.action_label.setStyleSheet(self.active_string_style)
        # this will cause the above code to present top the user. I know. it's dumb
        QApplication.processEvents()

        success, message = self.obPathway.base_data_pathway(self.is_testing, table_selected)

        if success:
            self.message_scroll_text.append(['Ingest '+table_selected+' data', message, self.success_string_style1])

        else:
            self.message_scroll_text.append(['Ingest '+table_selected+' data', message, self.failure_string_style])
        self.message_number = self.message_scroll_text.index(self.message_scroll_text[-1])
        self.set_new_tooltip()

        self.completion_alert('Ingest '+table_selected+' data', message)

    def ingestion_buttons(self,column_pos):
        self.ingestion_options = ['1-Minimum Product Ingestion(3 steps)','2-Full Product Ingestion(5 steps)','3-Fill Product Attributes(2 steps)','4-Minimum Product Price(3 steps)','5-Base Pricing(1 step)','GSA Pricing','VA Pricing', 'HTME Pricing',
                                  'ECAT Pricing', 'FEDMALL Pricing']
        self.ingestion_options.sort()

        self.ingest_data_button = QPushButton('Ingest New Data')
        self.ingest_data_button.setStyleSheet('QPushButton{background-color: lightgreen}')

        self.ingest_data_button.setToolTip('For bulk ingestion of product data files.')

        self.ingest_data_button.clicked.connect(self.ingest_data_kickoff)
        self.layout.addWidget(self.ingest_data_button, 1, column_pos)

        self.ingestion_action_dropdown = QComboBox()
        self.ingestion_action_dropdown.addItems(self.ingestion_options)
        self.layout.addWidget(self.ingestion_action_dropdown, 2, column_pos)

    def ingest_data_kickoff(self):
        ingestion_action_selected = str(self.ingestion_action_dropdown.currentText())
        self.action_label.setText('Ingestion: {}.'.format(ingestion_action_selected))
        self.action_label.setStyleSheet(self.active_string_style)
        # this will cause the above code to present to the user. I know. it's dumb
        QApplication.processEvents()

        success, message = self.obPathway.ingest_data_pathway(self.is_testing, ingestion_action_selected)

        if success:
            self.message_scroll_text.append([ingestion_action_selected,message,self.success_string_style1])

        else:
            self.message_scroll_text.append([ingestion_action_selected,message,self.failure_string_style])
        self.message_number = self.message_scroll_text.index(self.message_scroll_text[-1])
        self.set_new_tooltip()

        self.completion_alert(ingestion_action_selected, message)


    def update_data_buttons(self,column_pos):
        self.update_data_options = ['1-Update Minimum Product Data(3 steps)', '1.5-Update Minimum Product Price Data(2 steps)', '2-Update Full Product(5 steps)', '3-Update Product Attributes(2 steps)', '4-Update Base Pricing(1 step)', 'Update GSA Pricing', 'Update VA Pricing', 'Update HTME Pricing', 'Update ECAT Pricing', 'Update FEDMALL Pricing']
        self.update_data_options.sort()

        self.update_data_button = QPushButton('Update Data')
        self.update_data_button.setStyleSheet('QPushButton{background-color: lightgreen}')

        self.update_data_button.setToolTip('For bulk updates of product data.')

        self.update_data_button.clicked.connect(self.update_data_kickoff)
        self.layout.addWidget(self.update_data_button, 1, column_pos)

        self.update_action_dropdown = QComboBox()
        self.update_action_dropdown.addItems(self.update_data_options)
        self.layout.addWidget(self.update_action_dropdown, 2, column_pos)

    def update_data_kickoff(self):
        update_action_selected = str(self.update_action_dropdown.currentText())
        self.action_label.setText('Update: {}.'.format(update_action_selected))
        self.action_label.setStyleSheet(self.active_string_style)
        # this will cause the above code to present top the user. I know. it's dumb
        QApplication.processEvents()

        success, message = self.obPathway.update_data_pathway(self.is_testing, update_action_selected)

        if success:
            self.message_scroll_text.append([update_action_selected,message,self.success_string_style1])

        else:
            self.message_scroll_text.append([update_action_selected,message,self.failure_string_style])
        self.message_number = self.message_scroll_text.index(self.message_scroll_text[-1])
        self.set_new_tooltip()

        self.completion_alert(update_action_selected, message)


    def contract_buttons(self,column_pos):
        self.contract_options = ['Ready to load-BC','Ready to load-GSA']
        self.contract_options.sort()

        self.contract_button = QPushButton('Post Processing Action')
        self.contract_button.setStyleSheet('QPushButton{background-color: lightgreen}')

        self.contract_button.setToolTip('For setting product visibilty.')

        self.contract_button.clicked.connect(self.contract_kickoff)
        self.layout.addWidget(self.contract_button, 1, column_pos)

        self.contract_dropdown = QComboBox()
        self.contract_dropdown.addItems(self.contract_options)
        self.layout.addWidget(self.contract_dropdown, 2, column_pos)

    def contract_kickoff(self):
        contract_selected = str(self.contract_dropdown.currentText())
        self.action_label.setText('Post Processing: {}.'.format(contract_selected))
        self.action_label.setStyleSheet(self.active_string_style)
        # this will cause the above code to present top the user. I know. it's dumb
        QApplication.processEvents()

        success, message = self.obPathway.contract_pathway(self.is_testing, contract_selected)

        if success:
            self.message_scroll_text.append([contract_selected,message,self.success_string_style1])

        else:
            self.message_scroll_text.append([contract_selected,message,self.failure_string_style])
        self.message_number = self.message_scroll_text.index(self.message_scroll_text[-1])
        self.set_new_tooltip()

        self.completion_alert(contract_selected, message)


    def completion_alert(self, action_selected, message):
        alert = QMessageBox()
        alert.setIcon(QMessageBox.Information)
        window_title = 'Task: ' + action_selected
        alert.setWindowTitle(window_title)

        error_msg = 'Completion Message:\n' + message
        alert.setText(error_msg)

        x = alert.exec_()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
## end ##
