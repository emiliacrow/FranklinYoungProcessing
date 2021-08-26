# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20210813
# CreateFor: Franklin Young International


import pandas
import numpy as np

from Tools.FY_DAL import DalObject
from Tools.Validation import Validator
from Tools.Extraction import Extractor
from Tools.HeaderTranslator import HeaderTranslator
from Tools.Ingestion import IngestionObject
from Tools.ProgressBar import ProgressBarWindow
from Tools.ProgressBar import YesNoDialog
from Tools.ProgressBar import JoinSelectionDialog

class BasicProcessObject:
    # this object requires a list called req_fields which is the set of headers that must be present in order to process the file/line
    # we should instead generate a list of fields that will be taken from the original df based on what is available
    # it would have sets of fall backs such as: productPriceId is missing therefore we need FYPartNo to be able to do the look up
    #
    # the process would basically check the first set of 'can't run without these' headers
    # followed by the sets of required, but fall-backable headers
    # then any that might be useful
    # together this will generate the take-set from the original
    # this will be the run set required for line processing
    def __init__(self,df_product, is_testing):
        self.name = 'Bob'
        self.message = 'No message'
        self.success = False
        self.is_viable = False
        self.set_new_order = False
        self.is_last = False
        self.np_nan = np.nan
        self.df_product = df_product
        self.obHeaderTranslator = HeaderTranslator()

        self.lst_product_headers = self.obHeaderTranslator.translate_headers(list(self.df_product.columns))
        self.df_product.columns = self.lst_product_headers

        # remove duplicated columns by headers
        self.df_product = self.df_product.loc[:, ~self.df_product.columns.duplicated()]

        self.out_column_headers = self.df_product.columns
        self.return_df_product = pandas.DataFrame(columns=self.df_product.columns)

        self.header_viability()
        if self.is_viable:
            self.obDal = DalObject(is_testing)
            alive = self.obDal.ping_it()
            if alive == 'Ping':
                self.object_setup(is_testing)
            else:
                self.message = alive
                self.is_viable = False

    def header_viability(self):
        # if there are required headers we check if they're all there
        product_headers = set(self.lst_product_headers)
        if len(self.req_fields) > 0:
            required_headers = set(self.req_fields)
            self.is_viable = required_headers.issubset(product_headers)
            # if it passes and there are support headers we check them
            if (len(self.sup_fields) > 0) and self.is_viable:
                support_headers = set(self.sup_fields)
                if len(product_headers.intersection(support_headers)) == 0:
                    self.is_viable = False

        # there aren't required headers, but there are support headers
        elif len(self.sup_fields) > 0:
            support_headers = set(self.sup_fields)
            if len(product_headers.intersection(support_headers)) > 0:
                self.is_viable = True
        else:
            self.is_viable = True

    def get_missing_heads(self):
        product_headers = set(self.lst_product_headers)
        required_headers = set(self.req_fields)
        return list(required_headers.difference(product_headers))

    def object_setup(self,is_testing):
        self.obValidator = Validator()
        self.obExtractor = Extractor()
        self.obYNBox = YesNoDialog()

        self.obIngester = IngestionObject(self.obDal)
        self.df_country_translator = self.obIngester.get_country_lookup()
        self.df_category_names = self.obIngester.get_category_names()
        self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
        self.df_vendor_translator = self.obIngester.get_vendor_lookup()
        if 'FinalReport' not in self.lst_product_headers:
            self.df_product.insert(1, 'FinalReport','')
        if 'Report' not in self.lst_product_headers:
            self.df_product.insert(2, 'Report','')

    def set_progress_bar(self, count_of_steps, name):
        self.obProgressBarWindow = ProgressBarWindow(name)
        self.obProgressBarWindow.show()
        self.obProgressBarWindow.set_anew(count_of_steps)


    def begin_process(self):
        self.success = False
        if self.is_viable:
            self.success, self.message = self.run_process()
        elif self.message == 'No message':
            missing_heads = self.get_missing_heads()
            if len(missing_heads) == 1:
                self.message = 'The file is missing a product field: ' + missing_heads[0]
            elif len(missing_heads) != 0:
                self.message = 'The file is missing product fields: {} and {} more'.format(missing_heads[0],
                                                                                     str(len(missing_heads) - 1))
            else:
                self.message = 'The file is missing at least 1 supporting field.'

        return self.success, self.message

    def batch_preprocessing(self):
        pass


    def run_process(self):

        self.set_progress_bar(10, 'Batch preprocessing')
        self.obProgressBarWindow.update_unknown()
        self.batch_preprocessing()
        self.obProgressBarWindow.close()


        count_of_items = len(self.df_product.index)
        self.return_df_product = pandas.DataFrame(columns=self.out_column_headers)
        self.collect_return_dfs = []
        self.set_progress_bar(count_of_items, self.name)
        self.obProgressBarWindow.update_unknown()
        p_bar = 0
        good = 0
        bad = 0

        for colName, row in self.df_product.iterrows():
            # this takes one row and builds a df for a single product
            df_line_product = row.to_frame().T
            # this replaces empty string values with nan
            df_line_product = df_line_product.replace(r'^\s*$', self.np_nan, regex=True)
            # this removes all columns with all nan
            df_line_product = df_line_product.dropna(axis=1,how='all')

            if p_bar >= len(self.df_product.index)-1:
                self.is_last = True

            if self.line_viability(df_line_product):
                df_line_product['FinalReport'] = ['Passed Line Viability']

                success, return_df_line_product = self.process_product_line(df_line_product)
                if success:
                    return_df_line_product['FinalReport'] = 'Pass'
                else:
                    return_df_line_product['FinalReport'] = 'Failed'

            else:
                df_line_product['FinalReport'] = ['Failed Line Viability']
                success, return_df_line_product = self.report_missing_data(df_line_product)

            # appends all the product objects into a list
            self.collect_return_dfs.append(return_df_line_product)

            if success:
                good += 1
            else:
                bad += 1

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.set_progress_bar(10,'Appending data...')
        self.obProgressBarWindow.update_unknown()

        # this uses df.append to combine all the df product objects together
        self.return_df_product = self.return_df_product.append(self.collect_return_dfs)

        if self.set_new_order:
            matched_header_set = set(self.out_column_headers).union(set(self.return_df_product.columns))
            self.return_df_product = self.return_df_product[matched_header_set]

        self.obProgressBarWindow.close()

        self.df_product = self.return_df_product
        self.message = '{2}: {0} Fail, {1} Pass.'.format(bad,good,self.name)
        if good != 0:
            self.success = True

        return self.success, self.message

    def process_product_line(self, df_line_product):
        df_line_product['Report'] = ['Process not built']
        return False, df_line_product

    def process_boolean(self, df_collect_product_base_data, row, isCol):
        success = False
        if self.obValidator.validate_is_bool(row[isCol]):
            success = True
        else:
            df_collect_product_base_data['Report'] = [isCol + ' value out of range']
            self.bad_product_action(df_collect_product_base_data)

        return success, df_collect_product_base_data

    def process_attribute_data(self,df_line_product):
        df_collect_ids = df_line_product.copy()
        participant_attributes = list(set(self.att_fields).intersection(set(df_line_product.columns)))
        df_line_attributes = pandas.DataFrame(df_line_product[participant_attributes])

        if len(df_line_attributes.columns) > 0:
            for colName, row in df_line_attributes.iterrows():
                new_colName = row.index[0] + 'Id'
                if (new_colName not in row):
                    # this should be in validation as units validation or similar
                    term = self.obValidator.imperial_validation(row[row.index[0]])
                    if len(term) > 128:
                        term = term[:128]
                    new_id = self.obIngester.ingest_attribute(term,row.index[0])
                    df_collect_ids[new_colName] = [new_id]

        return df_collect_ids

    def process_manufacturer(self, df_collect_product_base_data, row):
        manufacturer = row['ManufacturerName']
        manufacturer = manufacturer.strip().replace('  ',' ')

        manufacturer_product_id = str(row['ManufacturerPartNumber'])

        if (manufacturer.lower() in self.df_manufacturer_translator['SupplierName'].values):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['SupplierName'] == manufacturer.lower()), ['ManufacturerId',
                                                                                    'FyManufacturerPrefix']].values[0]

            fy_catalog_number = self.make_fy_catalog_number(new_prefix, manufacturer_product_id)

            fy_product_number = fy_catalog_number

            if 'UnitOfMeasure' in row:
                unit_of_issue = row['UnitOfMeasure']
                if unit_of_issue != 'EA':
                    fy_product_number = fy_catalog_number + ' ' + unit_of_issue

            elif 'UnitOfIssue' in row:
                unit_of_issue = row['UnitOfIssue']
                if unit_of_issue != 'EA':
                    fy_product_number = fy_catalog_number + ' ' + unit_of_issue

            if 'FyPartNumber' not in row:
                df_collect_product_base_data['FyPartNumber'] = [fy_product_number]
            df_collect_product_base_data['FyProductNumber'] = [fy_product_number]
            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
            df_collect_product_base_data['FyCatalogNumber'] = [fy_catalog_number]
            return True, df_collect_product_base_data

        elif (manufacturer.upper() in self.df_manufacturer_translator['ManufacturerName'].unique()):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerName'] == manufacturer.upper()), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                0]

            fy_catalog_number = self.make_fy_catalog_number(new_prefix, manufacturer_product_id)

            fy_product_number = fy_catalog_number
            if 'UnitOfIssue' in row:
                unit_of_issue = row['UnitOfIssue']
                if unit_of_issue != 'EA':
                    fy_product_number = fy_catalog_number + ' ' + unit_of_issue

            elif 'UnitOfMeasure' in row:
                unit_of_issue = row['UnitOfMeasure']
                if unit_of_issue != 'EA':
                    fy_product_number = fy_catalog_number + ' ' + unit_of_issue


            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
            df_collect_product_base_data['FyCatalogNumber'] = [fy_catalog_number]

            return True, df_collect_product_base_data

        elif 'SupplierName' in row:
            supplier = row['SupplierName'].lower()
            if (supplier in self.df_manufacturer_translator['SupplierName'].values):
                new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == supplier), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                    0]

                fy_catalog_number = self.make_fy_catalog_number(new_prefix, manufacturer_product_id)

                fy_product_number = fy_catalog_number
                if 'UnitOfIssue' in row:
                    unit_of_issue = row['UnitOfIssue']
                    if unit_of_issue != 'EA':
                        fy_product_number = fy_catalog_number + ' ' + unit_of_issue

                elif 'UnitOfMeasure' in row:
                    unit_of_issue = row['UnitOfMeasure']
                    if unit_of_issue != 'EA':
                        fy_product_number = fy_catalog_number + ' ' + unit_of_issue

                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]
                df_collect_product_base_data['FyCatalogNumber'] = [fy_catalog_number]

                return True, df_collect_product_base_data

        else:
            df_collect_product_base_data['Report'] = ['New manufacturer']
            return False, df_collect_product_base_data

    def make_fy_catalog_number(self,prefix,manufacturer_part_number):
        clean_part_number = self.obValidator.clean_part_number(manufacturer_part_number)
        FY_catalog_number = str(prefix)+'-'+clean_part_number
        return FY_catalog_number

    def line_viability(self,df_product_line):
        # line viability checks
        line_headers = set(list(df_product_line.columns))
        required_headers = set(self.req_fields)
        return required_headers.issubset(line_headers)

    def report_missing_data(self, df_line_product):
        line_headers = set(list(df_line_product.columns))
        required_headers = set(self.req_fields)
        missing_headers = list(required_headers.difference(line_headers))
        report = 'Missing Data: ' + str(missing_headers)[1:-1]
        df_line_product['Report'] = [report]
        return False, df_line_product

    def get_df(self):
        return self.df_product





# future objects
# and format

class GSAPrice(BasicProcessObject):
    req_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product):
        super().__init__(df_product)
        self.name = 'GSA Price Ingestion'

    def process_product_line(self, return_df_line_product):
        return_df_line_product['Report'] = ['Process not built']
        return False, return_df_line_product

    # This will use the ingest function update_fks_base_price
    # as will all the other pricing pathways

## class add more here


## end ##




