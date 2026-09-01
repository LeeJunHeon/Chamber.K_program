# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'base_uiZOMQdw.ui'
##
## Created by: Qt User Interface Compiler version 6.9.1
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PyQt6.QtCore import (QCoreApplication, QMetaObject, QRect, Qt)
from PyQt6.QtGui import (QFont)
from PyQt6.QtWidgets import (QComboBox, QFrame, QLabel, QCheckBox,
    QLineEdit, QPlainTextEdit, QPushButton, QTextEdit)

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        if not Dialog.objectName():
            Dialog.setObjectName(u"Dialog")
        Dialog.resize(1041, 609)
        self.pushButton = QPushButton(Dialog)
        self.pushButton.setObjectName(u"pushButton")
        self.pushButton.setEnabled(False)
        self.pushButton.setGeometry(QRect(20, 120, 81, 51))
        self.pushButton.setStyleSheet(u"QPushButton {background: #ebebe9; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #cccccc;}")
        self.pushButton_2 = QPushButton(Dialog)
        self.pushButton_2.setObjectName(u"pushButton_2")
        self.pushButton_2.setEnabled(False)
        self.pushButton_2.setGeometry(QRect(20, 190, 81, 51))
        self.pushButton_2.setStyleSheet(u"QPushButton {background: #ebebe9; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #cccccc;}")
        self.Chamber_Button = QPushButton(Dialog)
        self.Chamber_Button.setObjectName(u"Chamber_Button")
        self.Chamber_Button.setGeometry(QRect(420, 120, 161, 111))
        self.Chamber_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.Chamber_Button.setCheckable(True)
        self.Chamber_Button.setEnabled(False)
        self.Ar_Button = QPushButton(Dialog)
        self.Ar_Button.setObjectName(u"Ar_Button")
        self.Ar_Button.setGeometry(QRect(200, 120, 81, 51))
        self.Ar_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.Ar_Button.setCheckable(True)
        self.O2_Button = QPushButton(Dialog)
        self.O2_Button.setObjectName(u"O2_Button")
        self.O2_Button.setGeometry(QRect(200, 190, 81, 51))
        self.O2_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.O2_Button.setCheckable(True)
        self.RV_button = QPushButton(Dialog)
        self.RV_button.setObjectName(u"RV_button")
        self.RV_button.setGeometry(QRect(280, 260, 81, 51))
        self.RV_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.RV_button.setCheckable(True)
        self.FV_button = QPushButton(Dialog)
        self.FV_button.setObjectName(u"FV_button")
        self.FV_button.setGeometry(QRect(280, 380, 81, 51))
        self.FV_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.FV_button.setCheckable(True)
        self.Turbo_button = QPushButton(Dialog)
        self.Turbo_button.setObjectName(u"Turbo_button")
        self.Turbo_button.setGeometry(QRect(460, 360, 81, 111))
        self.Turbo_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.Turbo_button.setCheckable(True)
        self.MV_button = QPushButton(Dialog)
        self.MV_button.setObjectName(u"MV_button")
        self.MV_button.setGeometry(QRect(460, 300, 81, 51))
        self.MV_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.MV_button.setCheckable(True)
        self.Rotary_button = QPushButton(Dialog)
        self.Rotary_button.setObjectName(u"Rotary_button")
        self.Rotary_button.setGeometry(QRect(20, 380, 81, 51))
        self.Rotary_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.Rotary_button.setCheckable(True)
        self.MS_button = QPushButton(Dialog)
        self.MS_button.setObjectName(u"MS_button")
        self.MS_button.setGeometry(QRect(20, 540, 81, 51))
        self.MS_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.MS_button.setCheckable(True)
        self.S1_button = QPushButton(Dialog)
        self.S1_button.setObjectName(u"S1_button")
        self.S1_button.setGeometry(QRect(130, 540, 81, 51))
        self.S1_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.S1_button.setCheckable(True)
        self.S2_button = QPushButton(Dialog)
        self.S2_button.setObjectName(u"S2_button")
        self.S2_button.setGeometry(QRect(240, 540, 81, 51))
        self.S2_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.S2_button.setCheckable(True)
        self.ION_button = QPushButton(Dialog)
        self.ION_button.setObjectName(u"ION_button")
        self.ION_button.setGeometry(QRect(250, 180, 81, 51))
        self.ION_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.ION_button.setCheckable(True)
        self.Vent_button = QPushButton(Dialog)
        self.Vent_button.setObjectName(u"Vent_button")
        self.Vent_button.setGeometry(QRect(620, 190, 81, 51))
        self.Vent_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #229b12;}")
        self.Vent_button.setCheckable(True)
        self.BuzzStop_Button = QPushButton(Dialog)
        self.BuzzStop_Button.setObjectName(u"BuzzStop_Button")
        self.BuzzStop_Button.setGeometry(QRect(590, 20, 131, 81))
        self.BuzzStop_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 13pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:checked {background: #32FF32; color: black; font-weight: bold; font-size: 13pt; border-radius: 8px; border: 2px solid #12790b;}")
        self.BuzzStop_Button.setCheckable(True)
        self.ALL_STOP_button = QPushButton(Dialog)
        self.ALL_STOP_button.setObjectName(u"ALL_STOP_button")
        self.ALL_STOP_button.setGeometry(QRect(590, 390, 131, 81))
        self.ALL_STOP_button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: red; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:pressed {background: #808080; border-color: #333333;}")
        self.ALL_STOP_button.setCheckable(False)
        self.Door_Button = QPushButton(Dialog)
        self.Door_Button.setObjectName(u"Door_Button")
        self.Door_Button.setGeometry(QRect(440, 80, 121, 51))
        self.Door_Button.setStyleSheet(u"QPushButton {\n"
"background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #f0f0f0, stop:1 #A0A0A0); \n"
"color: white;\n"
"font-weight: bold; font-size: 18pt;\n"
"border-radius: 20px;\n"
"padding: 8px 0;\n"
"border: 3px solid #555;\n"
"}\n"
"QPushButton:checked {\n"
"    background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #32FF32, stop:1 #229b12); \n"
"    color: black;\n"
"}")
        self.Door_Button.setCheckable(True)
        self.pushButton_3 = QPushButton(Dialog)
        self.pushButton_3.setObjectName(u"pushButton_3")
        self.pushButton_3.setEnabled(False)
        self.pushButton_3.setGeometry(QRect(90, 130, 121, 31))
        self.pushButton_3.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_4 = QPushButton(Dialog)
        self.pushButton_4.setObjectName(u"pushButton_4")
        self.pushButton_4.setEnabled(False)
        self.pushButton_4.setGeometry(QRect(270, 130, 111, 31))
        self.pushButton_4.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_5 = QPushButton(Dialog)
        self.pushButton_5.setObjectName(u"pushButton_5")
        self.pushButton_5.setEnabled(False)
        self.pushButton_5.setGeometry(QRect(90, 200, 121, 31))
        self.pushButton_5.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_6 = QPushButton(Dialog)
        self.pushButton_6.setObjectName(u"pushButton_6")
        self.pushButton_6.setEnabled(False)
        self.pushButton_6.setGeometry(QRect(270, 200, 361, 31))
        self.pushButton_6.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_7 = QPushButton(Dialog)
        self.pushButton_7.setObjectName(u"pushButton_7")
        self.pushButton_7.setEnabled(False)
        self.pushButton_7.setGeometry(QRect(340, 150, 41, 61))
        self.pushButton_7.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_9 = QPushButton(Dialog)
        self.pushButton_9.setObjectName(u"pushButton_9")
        self.pushButton_9.setEnabled(False)
        self.pushButton_9.setGeometry(QRect(480, 200, 41, 171))
        self.pushButton_9.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_11 = QPushButton(Dialog)
        self.pushButton_11.setObjectName(u"pushButton_11")
        self.pushButton_11.setEnabled(False)
        self.pushButton_11.setGeometry(QRect(90, 390, 381, 31))
        self.pushButton_11.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_12 = QPushButton(Dialog)
        self.pushButton_12.setObjectName(u"pushButton_12")
        self.pushButton_12.setEnabled(False)
        self.pushButton_12.setGeometry(QRect(220, 270, 41, 151))
        self.pushButton_12.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")
        self.pushButton_13 = QPushButton(Dialog)
        self.pushButton_13.setObjectName(u"pushButton_13")
        self.pushButton_13.setEnabled(False)
        self.pushButton_13.setGeometry(QRect(220, 270, 265, 31))
        self.pushButton_13.setStyleSheet(u"QPushButton {background: #b2b2b2; color: #888; font-weight: bold; font-size: 18pt; border: none; border-radius: 8px;}")

        # --- Process List / Select File (CSV) ---
        self.process_list_label = QLabel(Dialog)
        self.process_list_label.setObjectName(u"process_list_label")
        self.process_list_label.setGeometry(QRect(575, 270, 130, 32))
        self.process_list_label.setStyleSheet(u"font-size: 15pt;")
        self.process_list_label.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )

        self.select_csv_button = QPushButton(Dialog)
        self.select_csv_button.setObjectName(u"select_csv_button")
        self.select_csv_button.setGeometry(QRect(590, 300, 130, 32))
        self.select_csv_button.setStyleSheet(
            "QPushButton {background: #ebebe9; color: black; font-size: 15pt; "
            "border-radius: 6px; border: 1px solid #cccccc;}"
            "QPushButton:pressed {background: #dcdcdc;}"
        )

        self.Air_Indicator = QFrame(Dialog)
        self.Air_Indicator.setObjectName(u"Air_Indicator")
        self.Air_Indicator.setGeometry(QRect(20, 20, 51, 51))
        self.Air_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 25px; border: 2px solid #333;")
        self.Air_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.Air_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.G1_Indicator = QFrame(Dialog)
        self.G1_Indicator.setObjectName(u"G1_Indicator")
        self.G1_Indicator.setGeometry(QRect(105, 20, 51, 51))
        self.G1_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 25px; border: 2px solid #333;")
        self.G1_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.G1_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.G2_Indicator = QFrame(Dialog)
        self.G2_Indicator.setObjectName(u"G2_Indicator")
        self.G2_Indicator.setGeometry(QRect(185, 20, 51, 51))
        self.G2_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 25px; border: 2px solid #333;")
        self.G2_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.G2_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.ATM_Indicator = QFrame(Dialog)
        self.ATM_Indicator.setObjectName(u"ATM_Indicator")
        self.ATM_Indicator.setGeometry(QRect(265, 20, 51, 51))
        self.ATM_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 25px; border: 2px solid #333;")
        self.ATM_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.ATM_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.Water_Indicator = QFrame(Dialog)
        self.Water_Indicator.setObjectName(u"Water_Indicator")
        self.Water_Indicator.setGeometry(QRect(350, 20, 51, 51))
        self.Water_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 25px; border: 2px solid #333;")
        self.Water_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.Water_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.ION_RUN_Indicator = QFrame(Dialog)
        self.ION_RUN_Indicator.setObjectName(u"ION_RUN_Indicator")
        self.ION_RUN_Indicator.setGeometry(QRect(20, 110, 41, 41))
        self.ION_RUN_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 20px; border: 2px solid #333;")
        self.ION_RUN_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.ION_RUN_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.ION_RUN_label = QLabel(Dialog)
        self.ION_RUN_label.setObjectName(u"ION_RUN_label")
        self.ION_RUN_label.setGeometry(QRect(10, 152, 61, 18))
        self.ION_RUN_label.setStyleSheet(u"font-weight: bold; font-size: 10pt;")
        self.ION_RUN_label.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.ION_LAMP_Indicator = QFrame(Dialog)
        self.ION_LAMP_Indicator.setObjectName(u"ION_LAMP_Indicator")
        self.ION_LAMP_Indicator.setGeometry(QRect(105, 110, 41, 41))
        self.ION_LAMP_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 20px; border: 2px solid #333;")
        self.ION_LAMP_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.ION_LAMP_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.ION_LAMP_label = QLabel(Dialog)
        self.ION_LAMP_label.setObjectName(u"ION_LAMP_label")
        self.ION_LAMP_label.setGeometry(QRect(95, 152, 61, 18))
        self.ION_LAMP_label.setStyleSheet(u"font-weight: bold; font-size: 10pt;")
        self.ION_LAMP_label.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.ION_OT_Indicator = QFrame(Dialog)
        self.ION_OT_Indicator.setObjectName(u"ION_OT_Indicator")
        self.ION_OT_Indicator.setGeometry(QRect(185, 110, 41, 41))
        self.ION_OT_Indicator.setStyleSheet(u"background: #d6252f; border-radius: 20px; border: 2px solid #333;")
        self.ION_OT_Indicator.setFrameShape(QFrame.Shape.StyledPanel)
        self.ION_OT_Indicator.setFrameShadow(QFrame.Shadow.Raised)
        self.ION_OT_label = QLabel(Dialog)
        self.ION_OT_label.setObjectName(u"ION_OT_label")
        self.ION_OT_label.setGeometry(QRect(175, 152, 61, 18))
        self.ION_OT_label.setStyleSheet(u"font-weight: bold; font-size: 10pt;")
        self.ION_OT_label.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.label_2 = QLabel(Dialog)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setGeometry(QRect(20, 80, 51, 20))
        self.label_2.setStyleSheet(u"font-weight: bold; font-size: 13pt;")
        self.label_2.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.label_3 = QLabel(Dialog)
        self.label_3.setObjectName(u"label_3")
        self.label_3.setGeometry(QRect(105, 80, 51, 20))
        self.label_3.setStyleSheet(u"font-weight: bold; font-size: 13pt;")
        self.label_3.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.label_4 = QLabel(Dialog)
        self.label_4.setObjectName(u"label_4")
        self.label_4.setGeometry(QRect(185, 80, 51, 20))
        self.label_4.setStyleSheet(u"font-weight: bold; font-size: 13pt;")
        self.label_4.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.label_5 = QLabel(Dialog)
        self.label_5.setObjectName(u"label_5")
        self.label_5.setGeometry(QRect(265, 80, 51, 20))
        self.label_5.setStyleSheet(u"font-weight: bold; font-size: 13pt;")
        self.label_5.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.label_6 = QLabel(Dialog)
        self.label_6.setObjectName(u"label_6")
        self.label_6.setGeometry(QRect(350, 80, 51, 20))
        self.label_6.setStyleSheet(u"font-weight: bold; font-size: 13pt;")
        self.label_6.setAlignment(Qt.AlignmentFlag.AlignHCenter|Qt.AlignmentFlag.AlignTop)
        self.error_monitor = QTextEdit(Dialog)
        self.error_monitor.setObjectName(u"error_monitor")
        self.error_monitor.setGeometry(QRect(350, 480, 370, 115))
        self.error_monitor.setStyleSheet(u"background: white; color: black; font-size: 9pt; font-family: Consolas,monospace;")
        self.error_monitor.setReadOnly(True)
        self.stage_monitor = QTextEdit(Dialog)
        self.stage_monitor.setObjectName(u"stage_monitor")
        self.stage_monitor.setGeometry(QRect(20, 480, 320, 40))  # 위치/크기 조절 가능
        self.stage_monitor.setStyleSheet("background: white; color: black; font-size: 16pt; font-family: Consolas,monospace;")
        self.stage_monitor.setReadOnly(True)
        self.stage_monitor.setText("")  # 초기값 비워두기

        # =================================================================== #
        # START: Sputtering Panel UI 수정
        # =================================================================== #

        # --- Title ---
        self.sputtering_label = QLabel(Dialog)
        self.sputtering_label.setObjectName(u"sputtering_label")
        self.sputtering_label.setGeometry(QRect(760, 10, 271, 41))
        font = QFont()
        font.setPointSize(24)
        font.setBold(True)
        self.sputtering_label.setFont(font)
        self.sputtering_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # --- Target Pressure ---
        # self.target_pressure_label = QLabel(Dialog)
        # self.target_pressure_label.setObjectName(u"target_pressure_label")
        # self.target_pressure_label.setGeometry(QRect(760, 60, 121, 20))
        # self.Target_pressure_edit = QPlainTextEdit(Dialog)
        # self.Target_pressure_edit.setObjectName(u"Target_pressure_edit")
        # self.Target_pressure_edit.setGeometry(QRect(760, 80, 271, 31))

        
        # self.Ar_gas_label = QLabel(Dialog)
        # self.Ar_gas_label.setObjectName(u"Ar_gas_label")
        # self.Ar_gas_label.setGeometry(QRect(760, 120, 51, 20))
        # self.O2_gas_label = QLabel(Dialog)
        # self.O2_gas_label.setObjectName(u"O2_gas_label")
        # self.O2_gas_label.setGeometry(QRect(895, 120, 51, 20))

        # --- G1, G2 ---
        self.G1_checkbox = QCheckBox(Dialog)
        self.G1_checkbox.setObjectName(u"G1_checkbox")
        self.G1_checkbox.setGeometry(QRect(760, 75, 100, 26))
        self.G1_checkbox.setText("G1 Target")
        self.G2_checkbox = QCheckBox(Dialog)
        self.G2_checkbox.setObjectName(u"G2_checkbox")
        self.G2_checkbox.setGeometry(QRect(900, 75, 100, 26))
        self.G2_checkbox.setText("G2 Target")

        self.G1_label = QLabel(Dialog)
        self.G1_label.setObjectName(u"G1_label")
        self.G1_label.setGeometry(QRect(790, 98, 120, 20))
        self.G1_edit = QPlainTextEdit(Dialog)
        self.G1_edit.setObjectName(u"G1_edit")
        self.G1_edit.setGeometry(QRect(760, 98, 131, 31))
        self.G2_label = QLabel(Dialog)
        self.G2_label.setObjectName(u"G2_label")
        self.G2_label.setGeometry(QRect(925, 98, 120, 20))
        self.G2_edit = QPlainTextEdit(Dialog)
        self.G2_edit.setObjectName(u"G2_edit")
        self.G2_edit.setGeometry(QRect(900, 98, 131, 31))

        # --- Gas (Ar, O2) ---
        self.Ar_gas_radio = QCheckBox(Dialog)
        self.Ar_gas_radio.setObjectName(u"Ar_gas_radio")
        self.Ar_gas_radio.setGeometry(QRect(760, 133, 171, 26))
        #self.Ar_gas_radio.setChecked(True)

        self.O2_gas_radio = QCheckBox(Dialog)
        self.O2_gas_radio.setObjectName(u"O2_gas_radio")
        self.O2_gas_radio.setGeometry(QRect(900, 133, 171, 26))
        #self.O2_gas_radio.setChecked(False) 

        # --- Flow ---
        self.Ar_flow_edit = QPlainTextEdit(Dialog)
        self.Ar_flow_edit.setObjectName(u"Ar_flow_edit")
        self.Ar_flow_edit.setGeometry(QRect(760, 155, 131, 31))
        self.Ar_flow_edit.setPlainText("5")

        self.O2_flow_edit = QPlainTextEdit(Dialog)
        self.O2_flow_edit.setObjectName(u"O2_flow_edit")
        self.O2_flow_edit.setGeometry(QRect(900, 155, 131, 31))

        # --- Working Pressure ---
        self.working_pressure_label = QLabel(Dialog)
        self.working_pressure_label.setObjectName(u"working_pressure_label")
        self.working_pressure_label.setGeometry(QRect(760, 190, 181, 20))
        self.working_pressure_edit = QPlainTextEdit(Dialog)
        self.working_pressure_edit.setObjectName(u"working_pressure_edit")
        self.working_pressure_edit.setGeometry(QRect(760, 210, 271, 31))
        self.working_pressure_edit.setPlainText("2")

        # --- Power (RF, DC) ---
        self.rf_power_checkbox = QCheckBox(Dialog)
        self.rf_power_checkbox.setObjectName(u"rf_power_checkbox")
        self.rf_power_checkbox.setGeometry(QRect(760, 245, 131, 20))

        self.dc_power_checkbox = QCheckBox(Dialog)
        self.dc_power_checkbox.setObjectName(u"dc_power_checkbox")
        self.dc_power_checkbox.setGeometry(QRect(900, 245, 131, 20))

        self.RF_power_edit = QPlainTextEdit(Dialog)
        self.RF_power_edit.setObjectName(u"RF_power_edit")
        self.RF_power_edit.setGeometry(QRect(760, 265, 131, 31))
        self.RF_power_edit.setPlainText("200")

        self.DC_power_edit = QPlainTextEdit(Dialog)
        self.DC_power_edit.setObjectName(u"DC_power_edit")
        self.DC_power_edit.setGeometry(QRect(900, 265, 131, 31))
        self.DC_power_edit.setPlainText("200")

        # --- DC Power 안정화 대기 사용 여부 (기본 OFF) ---
        self.dc_delay_checkbox = QCheckBox(Dialog)
        self.dc_delay_checkbox.setObjectName(u"dc_delay_checkbox")
        self.dc_delay_checkbox.setGeometry(QRect(901, 298, 130, 22))
        self.dc_delay_checkbox.setChecked(False)

        # --- Shutter Delay ---
        self.shutter_delay_label = QLabel(Dialog)
        self.shutter_delay_label.setObjectName(u"shutter_delay_label")
        self.shutter_delay_label.setGeometry(QRect(760, 300, 141, 20))
        self.Shutter_delay_edit = QPlainTextEdit(Dialog)
        self.Shutter_delay_edit.setObjectName(u"Shutter_delay_edit")
        self.Shutter_delay_edit.setGeometry(QRect(760, 320, 271, 31))
        self.Shutter_delay_edit.setPlainText("5")

        # --- Process Time ---
        self.process_time_label = QLabel(Dialog)
        self.process_time_label.setObjectName(u"process_time_label")
        self.process_time_label.setGeometry(QRect(760, 355, 131, 20))
        self.process_time_edit = QPlainTextEdit(Dialog)
        self.process_time_edit.setObjectName(u"process_time_edit")
        self.process_time_edit.setGeometry(QRect(760, 375, 271, 31))
        self.process_time_edit.setPlainText("10")

        # --- Power Status (for.P, ref.P) ---
        self.for_p_label = QLabel(Dialog)
        self.for_p_label.setObjectName(u"for_p_label")
        self.for_p_label.setGeometry(QRect(760, 410, 50, 20))
        self.ref_p_label = QLabel(Dialog)
        self.ref_p_label.setObjectName(u"ref_p_label")
        self.ref_p_label.setGeometry(QRect(830, 410, 50, 20))
        self.offset_label = QLabel(Dialog)
        self.offset_label.setObjectName(u"offset_label")
        self.offset_label.setGeometry(QRect(900, 410, 50, 20))
        self.param_label = QLabel(Dialog)
        self.param_label.setObjectName(u"param_label")
        self.param_label.setGeometry(QRect(970, 410, 50, 20))

        self.for_p_edit = QPlainTextEdit(Dialog)
        self.for_p_edit.setObjectName(u"for_p_edit")
        self.for_p_edit.setGeometry(QRect(760, 430, 60, 31))
        self.ref_p_edit = QPlainTextEdit(Dialog)
        self.ref_p_edit.setObjectName(u"ref_p_edit")
        self.ref_p_edit.setGeometry(QRect(830, 430, 60, 31))
        self.offset_edit = QPlainTextEdit(Dialog)
        self.offset_edit.setObjectName(u"offset_edit")
        self.offset_edit.setGeometry(QRect(900, 430, 60, 31))
        self.offset_edit.setPlainText("6.79")
        self.param_edit = QPlainTextEdit(Dialog)
        self.param_edit.setObjectName(u"param_edit")
        self.param_edit.setGeometry(QRect(970, 430, 60, 31))
        self.param_edit.setPlainText("1.0395")
        
        # --- Power, Voltage, Current ---
        self.power_label = QLabel(Dialog)
        self.power_label.setObjectName(u"power_label")
        self.power_label.setGeometry(QRect(760, 465, 71, 20))
        self.voltage_label = QLabel(Dialog)
        self.voltage_label.setObjectName(u"voltage_label")
        self.voltage_label.setGeometry(QRect(855, 465, 71, 20))
        self.current_label = QLabel(Dialog)
        self.current_label.setObjectName(u"current_label")
        self.current_label.setGeometry(QRect(950, 465, 71, 20))
        self.Power_edit = QPlainTextEdit(Dialog)
        self.Power_edit.setObjectName(u"Power_edit")
        self.Power_edit.setGeometry(QRect(760, 485, 81, 31))
        self.Voltage_edit = QPlainTextEdit(Dialog)
        self.Voltage_edit.setObjectName(u"Voltage_edit")
        self.Voltage_edit.setGeometry(QRect(855, 485, 81, 31))
        self.Current_edit = QPlainTextEdit(Dialog)
        self.Current_edit.setObjectName(u"Current_edit")
        self.Current_edit.setGeometry(QRect(950, 485, 81, 31))

        # --- Start/Stop Buttons ---
        self.Sputter_Start_Button = QPushButton(Dialog)
        self.Sputter_Start_Button.setObjectName(u"Sputter_Start_Button")
        self.Sputter_Start_Button.setGeometry(QRect(760, 540, 130, 60))
        self.Sputter_Start_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:pressed {background: #808080; border-color: #333333;}")
        self.Sputter_Start_Button.setCheckable(False)
        self.Sputter_Stop_Button = QPushButton(Dialog)
        self.Sputter_Stop_Button.setObjectName(u"Sputter_Stop_Button")
        self.Sputter_Stop_Button.setGeometry(QRect(900, 540, 130, 60))
        self.Sputter_Stop_Button.setStyleSheet(u"QPushButton {background: #A0A0A0; color: red; font-weight: bold; font-size: 18pt; border-radius: 8px; border: 2px solid #555555;}\n"
"QPushButton:pressed {background: #808080; border-color: #333333;}")
        self.Sputter_Stop_Button.setCheckable(False)

        # ===================== 히터 (수동 제어 패널) =====================
        # [배치] 위 MFC(pushButton_2, y2=241)와 Rotary_button(y=380) 사이의
        #        여백 139px 정중앙에 배치.  y = 241 + (139-104)//2 = 258
        #        → 위 17px / 아래 18px 로 좌우 대칭에 가깝게 놓인다.
        # [구조] QFrame(heater_group)이 부모. 내부는 프레임 기준 상대 좌표.
        # [스타일] 우측 입력 패널과 같은 Qt 기본 폰트.
        #        ON 버튼만 장비 버튼(Ar/RV 등)의 체크색(#32FF32)을 따른다.
        # [입력창] QPlainTextEdit이 아닌 QLineEdit 사용.
        #        - QPlainTextEdit은 여러 줄 편집기라 스크롤바가 생기고
        #          텍스트가 위쪽에 붙는다. 한 줄 수치 입력에는 부적합.
        #        - QLineEdit은 세로 중앙 정렬이 기본이고 스크롤바가 없다.

        self.heater_group = QFrame(Dialog)
        self.heater_group.setObjectName(u"heater_group")
        self.heater_group.setGeometry(QRect(18, 258, 186, 104))
        self.heater_group.setFrameShape(QFrame.Shape.StyledPanel)
        self.heater_group.setStyleSheet(
            u"QFrame#heater_group {background: #ffffff; "
            u"border: 2px solid #cccccc; border-radius: 8px;}"
        )

        # --- 제목 ---
        self.heater_title_label = QLabel(self.heater_group)
        self.heater_title_label.setObjectName(u"heater_title_label")
        self.heater_title_label.setGeometry(QRect(10, 5, 166, 18))
        self.heater_title_label.setStyleSheet(
            u"QLabel {border: none; color: #333333; font-weight: bold;}"
        )

        # --- 1행: 현재 온도(읽기 전용) + 상태 ---
        self.heater_pv_title = QLabel(self.heater_group)
        self.heater_pv_title.setObjectName(u"heater_pv_title")
        self.heater_pv_title.setGeometry(QRect(10, 28, 30, 22))
        self.heater_pv_title.setStyleSheet(u"QLabel {border: none; color: #333333;}")

        self.heater_pv_edit = QLineEdit(self.heater_group)
        self.heater_pv_edit.setObjectName(u"heater_pv_edit")
        self.heater_pv_edit.setGeometry(QRect(42, 28, 52, 22))
        self.heater_pv_edit.setReadOnly(True)                       # PLC 값만 표시
        self.heater_pv_edit.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.heater_pv_edit.setStyleSheet(
            u"QLineEdit {background: #f0f0f0; border: 1px solid #cccccc; "
            u"border-radius: 3px; color: #333333;}"
        )

        # 상태 문구. 색상은 main.py의 update_heater_display()가 상황별로 덮어쓴다.
        self.heater_status_label = QLabel(self.heater_group)
        self.heater_status_label.setObjectName(u"heater_status_label")
        self.heater_status_label.setGeometry(QRect(100, 28, 76, 22))
        self.heater_status_label.setStyleSheet(u"QLabel {border: none; color: #333333;}")

        # --- 2행: 목표 온도 입력 + 적용 + ON ---
        self.heater_sv_title = QLabel(self.heater_group)
        self.heater_sv_title.setObjectName(u"heater_sv_title")
        self.heater_sv_title.setGeometry(QRect(10, 54, 30, 22))
        self.heater_sv_title.setStyleSheet(u"QLabel {border: none; color: #333333;}")

        self.heater_sv_edit = QLineEdit(self.heater_group)
        self.heater_sv_edit.setObjectName(u"heater_sv_edit")
        self.heater_sv_edit.setGeometry(QRect(42, 54, 52, 22))
        self.heater_sv_edit.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.heater_sv_edit.setMaxLength(6)                          # "180.0" 정도면 충분
        self.heater_sv_edit.setStyleSheet(
            u"QLineEdit {background: #ffffff; border: 1px solid #cccccc; "
            u"border-radius: 3px; color: #333333;}"
            u"QLineEdit:focus {border: 1px solid #4a90d9;}"
        )

        # [적용] 목표 온도만 다시 전송. 운전 중에도 목표 변경 가능.
        self.heater_apply_button = QPushButton(self.heater_group)
        self.heater_apply_button.setObjectName(u"heater_apply_button")
        self.heater_apply_button.setGeometry(QRect(100, 54, 36, 22))
        self.heater_apply_button.setStyleSheet(
            u"QPushButton {background: #ebebe9; color: black; font-weight: bold; "
            u"border-radius: 4px; border: 1px solid #cccccc;}"
            u"QPushButton:hover {background: #dcdcda;}"
        )

        # [ON] 체크 시 목표 전송 + HEATER_RUN ON. 체크색은 장비 버튼과 동일 규칙.
        self.heater_onoff_button = QPushButton(self.heater_group)
        self.heater_onoff_button.setObjectName(u"heater_onoff_button")
        self.heater_onoff_button.setGeometry(QRect(140, 54, 36, 22))
        self.heater_onoff_button.setCheckable(True)
        self.heater_onoff_button.setStyleSheet(
            u"QPushButton {background: #A0A0A0; color: white; font-weight: bold; "
            u"border-radius: 4px; border: 1px solid #555555;}"
            u"QPushButton:checked {background: #32FF32; color: black; "
            u"font-weight: bold; border-radius: 4px; border: 1px solid #229b12;}"
        )

        # --- 3행: PLC 내장 PID의 실제 출력 (DAC 카운트 + %) ---
        #     다른 라벨과 같은 #333333 을 써서 흐려 보이지 않게 한다.
        self.heater_mv_label = QLabel(self.heater_group)
        self.heater_mv_label.setObjectName(u"heater_mv_label")
        self.heater_mv_label.setGeometry(QRect(10, 80, 166, 18))
        self.heater_mv_label.setStyleSheet(u"QLabel {border: none; color: #333333;}")
        # ===================== 히터 =====================

        # =================================================================== #
        # END: Sputtering Panel UI 수정
        # =================================================================== #

        self.pushButton.raise_()
        self.pushButton_2.raise_()
        self.Chamber_Button.raise_()
        self.Ar_Button.raise_()
        self.O2_Button.raise_()
        self.RV_button.raise_()
        self.FV_button.raise_()
        self.Turbo_button.raise_()
        self.MV_button.raise_()
        self.Rotary_button.raise_()
        self.MS_button.raise_()
        self.S1_button.raise_()
        self.S2_button.raise_()
        self.ION_button.raise_()
        self.Vent_button.raise_()
        self.BuzzStop_Button.raise_()
        self.ALL_STOP_button.raise_()
        self.Door_Button.raise_()
        self.process_list_label.raise_()
        self.select_csv_button.raise_()
        self.Air_Indicator.raise_()
        self.G1_Indicator.raise_()
        self.G2_Indicator.raise_()
        self.ATM_Indicator.raise_()
        self.Water_Indicator.raise_()
        self.ION_RUN_Indicator.raise_()
        self.ION_LAMP_Indicator.raise_()
        self.ION_OT_Indicator.raise_()
        self.ION_RUN_label.raise_()
        self.ION_LAMP_label.raise_()
        self.ION_OT_label.raise_()
        self.label_2.raise_()
        self.label_3.raise_()
        self.label_4.raise_()
        self.label_5.raise_()
        self.label_6.raise_()
        self.error_monitor.raise_()
        self.pushButton_3.raise_()
        self.pushButton_4.raise_()
        self.pushButton_5.raise_()
        self.pushButton_6.raise_()
        self.pushButton_7.raise_()
        self.pushButton_9.raise_()
        self.pushButton_11.raise_()
        self.pushButton_12.raise_()
        self.pushButton_13.raise_()

        # === 이름 없는 버튼을 항상 맨 뒤로 보내기 ===
        for btn in [
            self.pushButton_3, self.pushButton_4, self.pushButton_5,
            self.pushButton_6, self.pushButton_7, self.pushButton_9,
            self.pushButton_11, self.pushButton_12, self.pushButton_13
        ]:
            btn.lower()

        self.retranslateUi(Dialog)
        # --- 인디케이터 모두 OFF(빨강)로 초기화 ---
        for name in ["Air", "G1", "G2", "ATM", "Water"]:
            self.set_indicator(name, False)

        QMetaObject.connectSlotsByName(Dialog)
    # setupUi

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QCoreApplication.translate("Dialog", u"Dialog", None))
        self.pushButton.setText(QCoreApplication.translate("Dialog", u"MFC", None))
        self.pushButton_2.setText(QCoreApplication.translate("Dialog", u"MFC", None))
        self.Chamber_Button.setText(QCoreApplication.translate("Dialog", u"Chamber", None))
        self.Ar_Button.setText(QCoreApplication.translate("Dialog", u"Ar", None))
        self.O2_Button.setText(QCoreApplication.translate("Dialog", u"O2", None))
        self.RV_button.setText(QCoreApplication.translate("Dialog", u"R. V.", None))
        self.FV_button.setText(QCoreApplication.translate("Dialog", u"F. V.", None))
        self.Turbo_button.setText(QCoreApplication.translate("Dialog", u"Turbo", None))
        self.MV_button.setText(QCoreApplication.translate("Dialog", u"M. V.", None))
        self.Rotary_button.setText(QCoreApplication.translate("Dialog", u"Rotary", None))
        self.MS_button.setText(QCoreApplication.translate("Dialog", u"M. S.", None))
        self.S1_button.setText(QCoreApplication.translate("Dialog", u"S1", None))
        self.S2_button.setText(QCoreApplication.translate("Dialog", u"S2", None))
        self.ION_button.setText(QCoreApplication.translate("Dialog", u"ION", None))
        self.Vent_button.setText(QCoreApplication.translate("Dialog", u"Vent", None))
        self.BuzzStop_Button.setText(QCoreApplication.translate("Dialog", u"Buzz Stop(1)", None))
        self.ALL_STOP_button.setText(QCoreApplication.translate("Dialog", u"ALL STOP", None))
        self.Door_Button.setText(QCoreApplication.translate("Dialog", u"Door", None))

        # --- 히터 패널 ---
        # 단위를 제목에 명시 → 각 입력창에서 단위 표기를 생략할 수 있음
        self.heater_title_label.setText(QCoreApplication.translate("Dialog", u"Heater [\u00b0C]", None))
        self.heater_pv_title.setText(QCoreApplication.translate("Dialog", u"\ud604\uc7ac", None))
        self.heater_sv_title.setText(QCoreApplication.translate("Dialog", u"\ubaa9\ud45c", None))
        # 초기 표시: 아직 PLC 연결 전이므로 '연결 대기' 상태임을 명시
        self.heater_status_label.setText(QCoreApplication.translate("Dialog", u"\uc5f0\uacb0 \ub300\uae30", None))
        self.heater_mv_label.setText(QCoreApplication.translate("Dialog", u"\ucd9c\ub825 : \uc815\uc9c0", None))
        self.heater_apply_button.setText(QCoreApplication.translate("Dialog", u"\uc801\uc6a9", None))
        self.heater_onoff_button.setText(QCoreApplication.translate("Dialog", u"ON", None))
        # QLineEdit은 placeholderText를 지원 → 빈 칸에 안내 문구 표시
        self.heater_pv_edit.setPlaceholderText(QCoreApplication.translate("Dialog", u"--.-", None))
        self.heater_sv_edit.setPlaceholderText(QCoreApplication.translate("Dialog", u"\uc628\ub3c4", None))

        # 툴팁: 조작 의미를 명확히 (수동 제어 시 오조작 방지)
        self.heater_pv_edit.setToolTip(QCoreApplication.translate("Dialog",
            u"PLC 열전대(TC ch0)가 읽는 현재 온도입니다. 표시 전용입니다.", None))
        self.heater_sv_edit.setToolTip(QCoreApplication.translate("Dialog",
            u"목표 온도(\u00b0C). PLC 내부 상한(HEATER_SV_LIMIT)으로 한 번 더 제한됩니다.", None))
        self.heater_apply_button.setToolTip(QCoreApplication.translate("Dialog",
            u"목표 온도만 다시 전송합니다. 운전 중에도 변경할 수 있습니다.", None))
        self.heater_onoff_button.setToolTip(QCoreApplication.translate("Dialog",
            u"히터 운전 ON/OFF. ON을 누르면 목표 온도를 전송한 뒤 운전을 시작합니다.\n"
            u"실제 온도 제어는 PLC 내장 PID가 수행합니다.", None))
        
        self.pushButton_3.setText("")
        self.pushButton_4.setText("")
        self.pushButton_5.setText("")
        self.pushButton_6.setText("")
        self.pushButton_7.setText("")
        self.pushButton_9.setText("")
        self.pushButton_11.setText("")
        self.pushButton_12.setText("")
        self.pushButton_13.setText("")
        self.process_list_label.setText(
            QCoreApplication.translate("Dialog", u"Process List", None)
        )
        self.select_csv_button.setText(
            QCoreApplication.translate("Dialog", u"Select File", None)
        )
        self.label_2.setText(QCoreApplication.translate("Dialog", u"Air", None))
        self.label_3.setText(QCoreApplication.translate("Dialog", u"G1", None))
        self.label_4.setText(QCoreApplication.translate("Dialog", u"G2", None))
        self.label_5.setText(QCoreApplication.translate("Dialog", u"ATM", None))
        self.label_6.setText(QCoreApplication.translate("Dialog", u"water", None))
        self.ION_RUN_label.setText(QCoreApplication.translate("Dialog", u"ION RUN", None))
        self.ION_LAMP_label.setText(QCoreApplication.translate("Dialog", u"ION LAMP", None))
        self.ION_OT_label.setText(QCoreApplication.translate("Dialog", u"ION O/T", None))

        # --- Sputtering Panel Text 수정 ---
        self.sputtering_label.setText(QCoreApplication.translate("Dialog", u"Sputtering", None))
        # self.target_pressure_label.setText(QCoreApplication.translate("Dialog", u"target pressure", None))
        # self.Ar_gas_label.setText(QCoreApplication.translate("Dialog", u"Ar", None))
        # self.O2_gas_label.setText(QCoreApplication.translate("Dialog", u"O2", None))
        
        # ▶ Ar/O2 텍스트를 체크박스에 표시
        self.Ar_gas_radio.setText(
            QCoreApplication.translate("Dialog", u"Ar flow [sccm]", None)
        )
        self.O2_gas_radio.setText(
            QCoreApplication.translate("Dialog", u"O2 flow [sccm]", None)
        )

        self.working_pressure_label.setText(
            QCoreApplication.translate("Dialog", u"working pressure [mTorr]", None)
        )

        # ▶ RF/DC 텍스트도 체크박스에 표시
        self.rf_power_checkbox.setText(
            QCoreApplication.translate("Dialog", u"RF power", None)
        )
        self.dc_power_checkbox.setText(
            QCoreApplication.translate("Dialog", u"DC power", None)
        )

        self.dc_delay_checkbox.setText(
            QCoreApplication.translate("Dialog", u"DC stabilize", None)
        )
        self.shutter_delay_label.setText(QCoreApplication.translate("Dialog", u"Shutter delay [min]", None))
        self.process_time_label.setText(QCoreApplication.translate("Dialog", u"process time [min]", None))
        self.for_p_label.setText(QCoreApplication.translate("Dialog", u"for.P", None))
        self.ref_p_label.setText(QCoreApplication.translate("Dialog", u"ref.P", None))
        self.offset_label.setText(QCoreApplication.translate("Dialog", u"offset", None))
        self.param_label.setText(QCoreApplication.translate("Dialog", u"param", None))
        self.power_label.setText(QCoreApplication.translate("Dialog", u"Power", None))
        self.voltage_label.setText(QCoreApplication.translate("Dialog", u"Voltage", None))
        self.current_label.setText(QCoreApplication.translate("Dialog", u"Current", None))
        self.Sputter_Start_Button.setText(QCoreApplication.translate("Dialog", u"Start", None))
        self.Sputter_Stop_Button.setText(QCoreApplication.translate("Dialog", u"Stop", None))

    # retranslateUi

    def set_indicator(self, name, state: bool):
        color = "#38d62f" if state else "#d6252f"
        frame = getattr(self, f"{name}_Indicator")
        frame.setStyleSheet(f"background: {color}; border-radius: 25px; border: 2px solid #333;")


