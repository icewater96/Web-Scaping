# -*- coding: utf-8 -*-
"""
Created on Fri Oct 07 15:17:15 2016

@author: jllu
"""

# Send an email in outlook
import os
import win32com.client as win32
import win32ui
    
def outlook_is_running():
    try:
        win32ui.FindWindow(None, "Microsoft Outlook")
        return True
    except win32ui.error:
        return False


# Check if outlook is running. If not, open it.
if not outlook_is_running():
    os.startfile("outlook")


outlook = win32.Dispatch('outlook.application' )

mail = outlook.CreateItem(0)

mail.To = 'jiuliu.lu@beckman.com'

mail.Subject = 'test'

mail.HtmlBody = 'test test'

mail.send