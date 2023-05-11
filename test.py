import merge
import unittest
import pandas as pd


tdata_student = pd.DataFrame({
    'BadColumn': ['1', '2', '3', '4'],
    'Gender': ['1', '', '', ''],
    'gender:': ['', '', '2', '1'],
    'Gender - SIS': ['', '2', '', ''],

    })

class TestMergeCSV(unittest.TestCase):

    def test_combine_variants(self):
        td = pd.DataFrame({
            's-peff-q1': ['1', '2', '3', '', ''],
            's-peff-q10': ['1', '2', '3', '', ''],
            's-peff-q1-1': ['', '', '', '4', '5'],
            's-peff-q10-1': ['', '', '', '4', '5'],
        })
        td = merge.combine_variants(td)
        expected = pd.DataFrame({
            's-peff-q1': ['1', '2', '3', '4', '5'],
            's-peff-q10': ['1', '2', '3', '4', '5'],
        })
        notexpected = pd.DataFrame({
            's-peff-q1-1': ['1', '2', '3', '4', '5'],
            's-peff-q10-1': ['1', '2', '3', '4', '5'],
        })
        self.assertTrue(td.equals(expected))
        self.assertFalse(td.equals(notexpected))
    
    def test_clean_cols_student(self):
        td = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'LASID': ['1', '2', '3', '4', '5'],
            'Grade': ['1', '2', '3', '4', '5'],
            'Race': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
            'BadColumn': ['x', 'x', 'x', 'x', 'x'],
            'Gender - SIS': ['1', '2', '3', '', ''],
            'Gender': ['1', '2', '1', '2', '2'],
            'Response Id': ['1', '2', '3', '4', '5'],
            's-peff-q1': ['1', '2', '3', '4', '5'],
            's-peff-q10': ['1', '2', '3', '4', '5'],
            's-peff-q1.1': ['1', '2', '3', '4', '5'],
        })
        td = merge.clean_cols_student(td)
        expected = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'LASID': ['1', '2', '3', '4', '5'],
            'Grade': ['1', '2', '3', '4', '5'],
            'Race': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
            'Gender': ['1', '2', '1', '2', '2'],
            'Response Id': ['1', '2', '3', '4', '5'],
            's-peff-q1': ['1', '2', '3', '4', '5'],
            's-peff-q10': ['1', '2', '3', '4', '5'],
        })
        notexpected = pd.DataFrame({
            'BadColumn': ['x', 'x', 'x', 'x', 'x'],
            'Gender - SIS': ['1', '2', '3', '', ''],
            's-peff-q1.1': ['1', '2', '3', '4', '5'],
        })
        self.assertTrue(td.equals(expected), td)
        self.assertFalse(td.equals(notexpected), td)
    
    def test_clean_cols_teacher(self):
        td = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'BadColumn': ['x', 'x', 'x', 'x', 'x'],
            'Blah Blah Blah': ['x', 'x', 'x', 'x', 'x'],
            'Abbey Road': ['x', 'x', 'x', 'x', 'x'],
            'Please List Your Cats': ['x', 'x', 'x', 'x', 'x'],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
            't-peff-q1': ['1', '2', '3', '4', '5'],
            't-peff-q10': ['1', '2', '3', '4', '5'],
            't-peff-q1.1': ['1', '2', '3', '4', '5'],
            't-peff-q10.1': ['1', '2', '3', '4', '5'],
        })
        td = merge.clean_cols_teacher(td)
        expected = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
            't-peff-q1': ['1', '2', '3', '4', '5'],
            't-peff-q10': ['1', '2', '3', '4', '5'],
        })
        notexpected = pd.DataFrame({
            'BadColumn': ['x', 'x', 'x', 'x', 'x'],
            'Blah Blah Blah': ['x', 'x', 'x', 'x', 'x'],
            'Abbey Road': ['x', 'x', 'x', 'x', 'x'],
            'Please List Your Cats': ['x', 'x', 'x', 'x', 'x'],
            't-peff-q1.1': ['1', '2', '3', '4', '5'],
            't-peff-q10.1': ['1', '2', '3', '4', '5'],
        })
        self.assertTrue(td.equals(expected), td)
        self.assertFalse(td.equals(notexpected), td)
    
    def test_combine_cols(self):
        td = pd.DataFrame({
            'My Column': ['1', '', '', '', ''],
            'My Other Column': ['', '2', '3', '', ''],
            'Not My Column': ['1', '2', '3', '4', '5'],
            'My Last Column': ['', '', '', '4', '5'],
        })
        expected = pd.DataFrame({
            'My Column': ['1', '2', '3', '4', '5'],
            'Not My Column': ['1', '2', '3', '4', '5'],
        })
        td = merge.combine_cols(td, 'My Column', ['my other column', 'my last column'])
        self.assertTrue(td.equals(expected), f'\n{td}')
    
    def test_repair_cols_student(self):
        td = pd.DataFrame({
            'Start Date': ['', '', '', '4', '5'],
            'End Date': ['', '', '', '4', '5'],
            'Ip Address': ['', '', '', '4', '5'],
            'StartDate': ['1', '2', '3', '', ''],
            'EndDate': ['1', '2', '3', '', ''],
            'IpAddress': ['1', '2', '3', '', ''],
            'Status': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['', '', '', '4', '5'],
            'RecordedDate': ['1', '2', '3', '', ''],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['', '', '', '4', '5'],
            'School': ['1', '2', '3', '', ''],
            'LASID': ['1', '2', '3', '', ''],
            'Please enter your Locally Assigned Student ID Number (LASID, or student lunch number).': ['', '', '', '4', '5'],
            'Grade': ['1', '2', '3', '', ''],
            'What grade are you in?': ['', '', '', '4', '5'],
            'Gender': ['1', '2', '3', '', ''],
            'What is your gender?': ['', '', '', '4', '5'],
            'Race': ['1', '2', '3', '4', '5'],
        })
        expected = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
            'LASID': ['1', '2', '3', '4', '5'],
            'Grade': ['1', '2', '3', '4', '5'],
            'Gender': ['1', '2', '3', '4', '5'],
            'Race': ['1', '2', '3', '4', '5'],
        })
        td = merge.repair_student_columns(td)
        self.assertTrue(td.equals(expected), f'\n{td}')
    
    def test_repair_cols_teacher(self):
        td = pd.DataFrame({
            'Start Date': ['', '', '', '4', '5'],
            'End Date': ['', '', '', '4', '5'],
            'Ip Address': ['', '', '', '4', '5'],
            'StartDate': ['1', '2', '3', '', ''],
            'EndDate': ['1', '2', '3', '', ''],
            'IpAddress': ['1', '2', '3', '', ''],
            'Status': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['', '', '', '4', '5'],
            'RecordedDate': ['1', '2', '3', '', ''],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['', '', '', '4', '5'],
            'School': ['1', '2', '3', '', ''],
        })
        expected = pd.DataFrame({
            'Start Date': ['1', '2', '3', '4', '5'],
            'End Date': ['1', '2', '3', '4', '5'],
            'Ip Address': ['1', '2', '3', '4', '5'],
            'Status': ['1', '2', '3', '4', '5'],
            'Progress': ['1', '2', '3', '4', '5'],
            'Duration': ['1', '2', '3', '4', '5'],
            'District': ['1', '2', '3', '4', '5'],
            'Recorded Date': ['1', '2', '3', '4', '5'],
            'Response Id': ['1', '2', '3', '4', '5'],
            'Dese Id': ['1', '2', '3', '4', '5'],
        })
        td = merge.repair_teacher_columns(td)
        self.assertTrue(td.equals(expected), td)
    
if __name__ == '__main__':
    unittest.main()