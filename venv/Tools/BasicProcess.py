# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20220318
# CreateFor: Franklin Young International


import pandas
import numpy as np

from Tools.FY_DAL import DalObject
from Tools.Validation import Validator
from Tools.Extraction import Extractor
from Tools.Ingestion import IngestionObject
from Tools.HeaderTranslator import HeaderTranslator

from Tools.ProgressBar import YesNoDialog
from Tools.ProgressBar import ProgressBarWindow
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
    def __init__(self, df_product, user, password, is_testing):
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
            self.obDal = DalObject(user, password)
            alive = self.obDal.ping_it(is_testing)
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

    def vendor_name_selection(self):
        lst_vendor_names = self.df_vendor_translator['VendorName'].tolist()
        self.obVendorTickBox = JoinSelectionDialog(lst_vendor_names, 'Please select 1 Vendor.')
        self.obVendorTickBox.exec()
        # split on column or column
        vendor_name_list = self.obVendorTickBox.get_selected_items()
        vendor_name = vendor_name_list[0]
        return vendor_name

    def object_setup(self,is_testing):
        self.obValidator = Validator()
        self.obExtractor = Extractor()
        self.obYNBox = YesNoDialog()

        self.obIngester = IngestionObject(self.obDal)
        self.df_country_translator = self.obIngester.get_country_lookup()
        self.df_category_names = self.obIngester.get_category_names()
        self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
        self.df_vendor_translator = self.obIngester.get_vendor_lookup()


    def set_progress_bar(self, count_of_steps, name):
        self.obProgressBarWindow = ProgressBarWindow(name)
        self.obProgressBarWindow.show()
        self.obProgressBarWindow.set_anew(count_of_steps)


    def batch_process_vendor(self):
        print('Vendor matching')
        df_attribute = self.df_product[['VendorName']]
        df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
        lst_ids = []
        for colName, row in df_attribute.iterrows():
            vendor_name = row['VendorName'].upper()
            if vendor_name in self.df_vendor_translator['VendorCode'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorCode'] == vendor_name),'VendorId'].values[0]
            elif vendor_name in self.df_vendor_translator['VendorName'].values:
                new_vendor_id = self.df_vendor_translator.loc[
                    (self.df_vendor_translator['VendorName'] == vendor_name),'VendorId'].values[0]
            else:
                vendor_name_list = self.df_vendor_translator["VendorName"].tolist()
                vendor_name_list = list(dict.fromkeys(vendor_name_list))

                new_vendor_id = self.obIngester.manual_ingest_vendor(atmp_name=vendor_name,atmp_code=vendor_name,lst_vendor_names=vendor_name_list)

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = self.df_product.merge(df_attribute, how='left', on=['VendorName'])


    def batch_process_manufacturer(self):
        print('Manufacturer matching')
        df_attribute = self.df_product[['ManufacturerName']]
        df_attribute = df_attribute.drop_duplicates(subset=['ManufacturerName'])
        lst_ids = []
        lst_names = []
        for colName, row in df_attribute.iterrows():
            manufacturer_name = row['ManufacturerName'].upper()
            manufacturer_name = self.obValidator.clean_manufacturer_name(manufacturer_name,True)
            while '  ' in manufacturer_name:
                manufacturer_name = manufacturer_name.replace('  ',' ')

            new_manufacturer_name = manufacturer_name
            if manufacturer_name.lower() in self.df_manufacturer_translator['SupplierName'].values:
                new_manufacturer_id = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == manufacturer_name.lower()),'ManufacturerId'].values[0]
                new_manufacturer_name = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == manufacturer_name.lower()),'ManufacturerName'].values[0]
            elif manufacturer_name in self.df_manufacturer_translator['ManufacturerName'].values:
                new_manufacturer_id = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['ManufacturerName'] == manufacturer_name),'ManufacturerId'].values[0]
            else:
            #    manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
            #    manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))
            #    new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=manufacturer_name.lower(), atmp_man=manufacturer_name, lst_manufacturer_names=manufacturer_name_list)
                new_manufacturer_id = -1

            lst_ids.append(new_manufacturer_id)
            lst_names.append(new_manufacturer_name)

        df_attribute['ManufacturerId'] = lst_ids
        df_attribute['UpdateManufacturerName'] = lst_names

        self.df_product = self.df_product.merge(df_attribute, how='left', on=['ManufacturerName'])

        self.df_product['ManufacturerName'] = self.df_product[['UpdateManufacturerName']]


    def define_new(self, b_match_vendor = False):
        if b_match_vendor:
            self.batch_process_vendor()
            self.batch_process_manufacturer()
        print('Agni Kai')
        """
          M C P V : types of product ids
        1 X X X X : These are called ready (unless they need pricing in which case Update-BasePrice)
        2 X X X O : these are from a different vendor, Update-productprice
        3 X X O X : these are a different size, Update-productprice
        4 X X O O : these are likely missing vendor info, Update-productprice
        5 X O     : These are likely overrides
        6 O X     : These are manufacturer name updates
        7 O O     : new
        """
        # get product look up
        self.df_product_agni_kai_lookup = self.obDal.get_product_action_review_lookup()
        # for final step
        self.df_product_notes = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['db_FyProductNotes']!= '')]
        drop_notes = ['ProductId', 'ManufacturerName', 'ManufacturerPartNumber', 'FyCatalogNumber','FyProductNumber',
                      'VendorName','VendorPartNumber','BaseProductPriceId','db_IsDiscontinued',
                      'ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId']
        self.df_product_notes = self.df_product_notes.drop(columns=drop_notes)
        self.df_product_agni_kai_lookup = self.df_product_agni_kai_lookup.drop(columns=['db_FyProductNotes'])

        self.df_product_agni_kai_lookup_copy = self.df_product_agni_kai_lookup.copy()
        drop_copy = ['ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId']
        self.df_product_agni_kai_lookup_copy = self.df_product_agni_kai_lookup_copy.drop(columns=drop_copy)

        if 'ProductId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = ['ProductId'])
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = ['ProductPriceId'])
        if 'BaseProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = ['BaseProductPriceId'])
        if 'db_IsDiscontinued' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = ['db_IsDiscontinued'])

        # set the full look up
        self.df_full_product_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['BaseProductPriceId'] != 'Load Pricing')]
        self.df_product_agni_kai_lookup = self.df_product_agni_kai_lookup[(self.df_product_agni_kai_lookup['BaseProductPriceId'] == 'Load Pricing')]
        self.df_product_agni_kai_lookup = self.df_product_agni_kai_lookup.drop(columns = ['BaseProductPriceId','ECATProductPriceId', 'HTMEProductPriceId','GSAProductPriceId','VAProductPriceId'])

        # first round filtering
        self.df_full_product_lookup['Filter'] = 'Ready'

        # match on everything
        print('Round 1')
        self.df_product = self.df_product.merge(self.df_full_product_lookup, how='left',on=['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'])
        # set aside the good matches
        self.df_full_matched_product = self.df_product[(self.df_product['Filter'] == 'Ready')]

        del self.df_full_product_lookup

        # prep next step data
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Ready')]
        self.df_product = self.df_product.drop(columns = ['Filter','ProductId','ProductPriceId','BaseProductPriceId','db_IsDiscontinued'])

        print('Round 2')
        self.df_product_agni_kai_lookup['Filter'] = 'Base Pricing'
        self.df_product = self.df_product.merge(self.df_product_agni_kai_lookup, how='left',on=['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'])

        del self.df_product_agni_kai_lookup

        # set aside the good matches
        self.df_pricing_matched_product = self.df_product[(self.df_product['Filter'] == 'Base Pricing')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Base Pricing')]

        self.df_product = self.df_product.drop(columns = ['Filter','ProductId','ProductPriceId','db_IsDiscontinued'])

        # round 3
        print('Round 3')
        self.df_product_agni_kai_lookup_copy['Filter'] = 'Partial'
        self.df_product = self.df_product.merge(self.df_product_agni_kai_lookup_copy, how='left',on=['FyCatalogNumber','ManufacturerPartNumber'])

        self.df_man_ven_matched_products = self.df_product[(self.df_product['Filter'] == 'Partial')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Partial')]

        if len(self.df_man_ven_matched_products.index) > 0:
            self.man_ven_cleanup()

        # set aside everything that didn't match
        # but wait! there's more!

        self.df_product['ManufacturerName'] = self.df_product[['ManufacturerName_x']]
        self.df_product['VendorPartNumber'] = self.df_product[['VendorPartNumber_x']]
        self.df_product['VendorName'] = self.df_product[['VendorName_x']]
        self.df_product['FyProductNumber'] = self.df_product[['FyProductNumber_x']]

        drop_columns = ['Filter','db_IsDiscontinued','ProductId','ProductPriceId','BaseProductPriceId',
                        'ECATProductPriceId', 'HTMEProductPriceId', 'GSAProductPriceId',
                        'ManufacturerName_x', 'ManufacturerName_y','VendorName_x','VendorName_y',
                        'FyProductNumber_x','FyProductNumber_y','VendorPartNumber_x','VendorPartNumber_y']
        self.df_product = self.df_product.drop(columns=drop_columns)

        print('Round 4')
        # print(self.df_product.columns)
        self.df_product = self.df_product.merge(self.df_product_agni_kai_lookup_copy, how='left',on=['FyCatalogNumber','ManufacturerName'])
        self.df_fy_cat_matched_products = self.df_product[(self.df_product['Filter'] == 'Partial')]

        del self.df_product_agni_kai_lookup_copy

        self.df_product = self.df_product[(self.df_product['Filter'] != 'Partial')]

        if len(self.df_fy_cat_matched_products.index) > 0:
            self.fy_cat_cleanup()

        # print('b',self.df_product.columns)

        self.df_product['ManufacturerPartNumber'] = self.df_product[['ManufacturerPartNumber_x']]
        self.df_product['VendorPartNumber'] = self.df_product[['VendorPartNumber_x']]
        self.df_product['VendorName'] = self.df_product[['VendorName_x']]
        self.df_product['FyProductNumber'] = self.df_product[['FyProductNumber_x']]

        drop_columns = ['Filter','db_IsDiscontinued','ProductId','ProductPriceId','BaseProductPriceId',
                        'ManufacturerPartNumber_x', 'ManufacturerPartNumber_y','VendorName_x','VendorName_y',
                        'FyProductNumber_x','FyProductNumber_y','VendorPartNumber_x','VendorPartNumber_y']
        self.df_product = self.df_product.drop(columns=drop_columns)
        # print('c',self.df_product.columns)

        self.df_product['Filter'] = 'New'

        print('Collecting')
        if len(self.df_full_matched_product.index) > 0:
            self.df_full_matched_product = self.df_full_matched_product.drop_duplicates()
            self.df_product = pandas.concat([self.df_product,self.df_full_matched_product], ignore_index = True)
        del self.df_full_matched_product

        if len(self.df_pricing_matched_product.index) > 0:
            self.df_pricing_matched_product = self.df_pricing_matched_product.drop_duplicates()
            self.df_product = pandas.concat([self.df_product,self.df_pricing_matched_product], ignore_index = True)
        del self.df_pricing_matched_product

        if len(self.df_man_ven_matched_products.index) > 0:
            self.df_man_ven_matched_products = self.df_man_ven_matched_products.drop_duplicates()
            self.df_product = pandas.concat([self.df_product,self.df_man_ven_matched_products], ignore_index = True)
        del self.df_man_ven_matched_products

        if len(self.df_fy_cat_matched_products.index) > 0:
            self.df_fy_cat_matched_products = self.df_fy_cat_matched_products.drop_duplicates()
            self.df_product = pandas.concat([self.df_product,self.df_fy_cat_matched_products], ignore_index = True)
        del self.df_fy_cat_matched_products


        if 'VendorPartNumber_x' in self.df_product.columns and 'VendorPartNumber' not in self.df_product.columns:
            self.df_product['VendorPartNumber'] = self.df_product['VendorPartNumber_x']
            self.df_product = self.df_product.drop(columns = ['VendorPartNumber_x'])

        if 'FyProductNumber_x' in self.df_product.columns and 'FyProductNumber' not in self.df_product.columns:
            self.df_product['FyProductNumber'] = self.df_product['FyProductNumber_x']
            self.df_product = self.df_product.drop(columns = ['FyProductNumber_x'])

        print('Finals')
        self.eval_cases()

        self.duplicate_logic()
        if 'ProductPriceId' in self.df_product.columns:
            print('Adding Notes')
            self.df_product = self.df_product.merge(self.df_product_notes, how='left',on=['ProductPriceId'])

        del self.df_product_notes
        self.df_product = self.df_product.reindex()


    def duplicate_logic(self):
        # it seems that this needs better returns for review
        # perhaps pull the
        # counts FyProductNumber occurance as series
        self.df_dupe_product = self.df_product.copy()

        cur_headers = set(self.df_dupe_product.columns)
        match_set = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'FyProductNumber', 'VendorName', 'VendorPartNumber']

        matched_headers = list(cur_headers.difference(set(match_set)))
        self.df_dupe_product = self.df_dupe_product.drop(columns=matched_headers)
        self.df_dupe_product.drop_duplicates(['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber'], inplace= True)

        self.srs_matched_product = self.df_dupe_product.loc[:, 'FyProductNumber'].value_counts()

        self.srs_matched_product.rename_axis()

        # sets series to dataframe
        self.df_matched_product = self.srs_matched_product.to_frame().reset_index()
        # names columns in new dataframe
        self.df_matched_product.rename(columns = {'FyProductNumber':'number','index':'FyProductNumber'}, inplace = 1)

        # assign duplicate marker
        self.df_matched_product['is_duplicated'] = 'Y'
        self.df_matched_product = self.df_matched_product.loc[(self.df_matched_product['number'] > 1),['FyProductNumber','is_duplicated']]

        # merge the duplicate mark back in
        self.df_product = self.df_product.merge(self.df_matched_product, how='left', on='FyProductNumber')

        self.df_product.loc[(self.df_product['is_duplicated'] == 'Y'), 'Filter'] = 'Possible Duplicate'

        self.df_product = self.df_product.drop(columns = ['is_duplicated'])

        # here we are going to match everything called new to the existing manufcaturer parts
        # this is to indicate the difference between the ingestable new products and updatable products
        if 'db_IsDiscontinued' in self.df_product.columns:
            self.df_product.loc[(self.df_product['db_IsDiscontinued'] == 'Y'), 'Alert'] = 'This product is currently discontinued'


    def eval_cases(self):
        # include an alert here that says what it is
        self.df_product.loc[(self.df_product['Filter'] == 'Ready'), 'Alert'] = 'Ready to update/contract'
        self.df_product.loc[(self.df_product['Filter'] == 'Base Pricing'), 'Alert'] = 'These go through update base pricing'
        self.df_product.loc[(self.df_product['Filter'] == 'Partial'), 'Alert'] = 'These can go through update step 1.5'

        self.df_product.loc[(self.df_product['Filter'] == 'case_1'), 'Alert'] = 'Vendor Part Number Change'

        # some of them are going to be changed
        self.df_product.loc[(self.df_product['Filter'] == 'case_2'), 'Alert'] = 'New Vendor for Existing Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_2'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_3'), 'Alert'] = 'New Vendor for New Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_3'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_6'), 'Alert'] = 'New Vendor for Existing Configuration(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_6'), 'Filter'] = 'Partial'


        self.df_product.loc[(self.df_product['Filter'] == 'case_4'), 'Alert'] = 'Configuration change-4'
        self.df_product.loc[(self.df_product['Filter'] == 'case_7'), 'Alert'] = 'Configuration change-7'
        self.df_product.loc[(self.df_product['Filter'] == 'case_7'), 'Filter'] = 'case_4'


        self.df_product.loc[(self.df_product['Filter'] == 'case_5'), 'Alert'] = 'Possible Override/Duplicate'
        self.df_product.loc[(self.df_product['Filter'] == 'case_5'), 'Filter'] = 'Possible_Duplicate'


        self.df_product.loc[(self.df_product['Filter'] == 'case_8'), 'Alert'] = 'New Vendor Existing product(step 1.5)'
        self.df_product.loc[(self.df_product['Filter'] == 'case_8'), 'Filter'] = 'Partial'

        self.df_product.loc[(self.df_product['Filter'] == 'case_9'), 'Alert'] = 'Verify your ManufacturerName'


        self.df_product.loc[(self.df_product['Filter'] == 'Possible_Duplicate'), 'TakePriority'] = 'Z'
        self.df_product.loc[(self.df_product['Filter'] == 'case_4'), 'TakePriority'] = 'W'
        self.df_product.loc[(self.df_product['Filter'] == 'case_9'), 'TakePriority'] = 'F'

        self.df_product.loc[(self.df_product['Filter'] == 'New'), 'TakePriority'] = 'E'
        self.df_product.loc[(self.df_product['Filter'] == 'Partial'), 'TakePriority'] = 'D'
        self.df_product.loc[(self.df_product['Filter'] == 'case_1'), 'TakePriority'] = 'C'
        self.df_product.loc[(self.df_product['Filter'] == 'Base Pricing'), 'TakePriority'] = 'B'
        self.df_product.loc[(self.df_product['Filter'] == 'Ready'), 'TakePriority'] = 'A'

        self.df_product.sort_values(by=['FyCatalogNumber','ManufacturerName','ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber','TakePriority'] , inplace = True)


    def man_ven_cleanup(self):
        # vendor part number change
        case_1 = ((self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] != self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']))

        # new vendor for existing configuration
        case_2 = ((self.df_man_ven_matched_products['VendorName_x'] != self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] == self.df_man_ven_matched_products['FyProductNumber_y']))

        # new vendor for new configuration
        case_3 = ((self.df_man_ven_matched_products['VendorName_x'] != self.df_man_ven_matched_products['VendorName_y']) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] != self.df_man_ven_matched_products['FyProductNumber_y']))

        # configuration change
        case_4 = ( (self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y'] ) &
                  (self.df_man_ven_matched_products['ManufacturerName_x'] == self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']) &
                  (self.df_man_ven_matched_products['FyProductNumber_x'] != self.df_man_ven_matched_products['FyProductNumber_y']))

        # BAAAAAD manufacturer
        case_9 = ((self.df_man_ven_matched_products['ManufacturerName_x'] != self.df_man_ven_matched_products['ManufacturerName_y']) &
                  (self.df_man_ven_matched_products['VendorName_x'] == self.df_man_ven_matched_products['VendorName_y'] ) &
                  (self.df_man_ven_matched_products['VendorPartNumber_x'] == self.df_man_ven_matched_products['VendorPartNumber_y']))

        conditions = [case_1,case_2,case_3,case_4,case_9]
        choices = ['case_1','case_2','case_3','case_4','case_9']

        self.df_man_ven_matched_products['Filter'] = np.select(conditions, choices, default='Partial')

        self.df_man_ven_matched_products = self.man_ven_match_x_y_cleaning(self.df_man_ven_matched_products)


    def fy_cat_cleanup(self):
        #  matched on 'FyCatalogNumber','ManufacturerName'
        # This is a likely override/duplicate
        case_5 = ((self.df_fy_cat_matched_products['VendorName_x'] == self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] != self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        # new vendor exising configuration
        case_6 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] == self.df_fy_cat_matched_products['FyProductNumber_y']))

        # Configuration change
        case_7 = ((self.df_fy_cat_matched_products['VendorName_x'] == self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] == self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] != self.df_fy_cat_matched_products['FyProductNumber_y']))

        # True partial
        case_8 = ((self.df_fy_cat_matched_products['VendorName_x'] != self.df_fy_cat_matched_products['VendorName_y']) &
                  (self.df_fy_cat_matched_products['ManufacturerPartNumber_x'] == self.df_fy_cat_matched_products['ManufacturerPartNumber_y']) &
                  (self.df_fy_cat_matched_products['VendorPartNumber_x'] != self.df_fy_cat_matched_products['VendorPartNumber_y']) &
                  (self.df_fy_cat_matched_products['FyProductNumber_x'] != self.df_fy_cat_matched_products['FyProductNumber_y']))

        conditions = [case_5,case_6,case_7,case_8]
        choices = ['case_5','case_6','case_7','case_8']

        self.df_fy_cat_matched_products['Filter'] = np.select(conditions, choices, default='other')

        # we will have to make assignments of more values _x, _y as we identify the partial type

        self.df_fy_cat_matched_products =self.x_y_cleaning(self.df_fy_cat_matched_products)


    def man_ven_match_x_y_cleaning(self, df_to_clean):
        # good: FyCatalogNumber, ManufacturerPartNumber, ProductId
        df_to_clean_case_1 = df_to_clean[(df_to_clean['Filter'] == 'case_1')].copy()
        df_to_clean_case_2 = df_to_clean[(df_to_clean['Filter'] == 'case_2')].copy()
        df_to_clean_case_3 = df_to_clean[(df_to_clean['Filter'] == 'case_3')].copy()
        df_to_clean_case_4 = df_to_clean[(df_to_clean['Filter'] == 'case_4')].copy()
        df_to_clean_case_9 = df_to_clean[(df_to_clean['Filter'] == 'case_9')].copy()
        df_partials = df_to_clean[(df_to_clean['Filter'] == 'Partial')].copy()

        if len(df_to_clean_case_1.index) > 0:
            # vendor part number change
            df_to_clean_case_1['ManufacturerName'] = df_to_clean_case_1[['ManufacturerName_x']]
            df_to_clean_case_1['FyProductNumber'] = df_to_clean_case_1[['FyProductNumber_x']]
            df_to_clean_case_1['VendorName'] = df_to_clean_case_1[['VendorName_x']]
            df_to_clean_case_1['VendorPartNumber'] = df_to_clean_case_1[['VendorPartNumber_x']]
            df_to_clean_case_1['CurrentVendorPartNumber'] = df_to_clean_case_1[['VendorPartNumber_y']]

        drop_1 = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_case_1 = df_to_clean_case_1.drop(columns = drop_1)

        if len(df_to_clean_case_2.index) > 0:
            # new vendor for existing configuration
            df_to_clean_case_2['ManufacturerName'] = df_to_clean_case_2[['ManufacturerName_x']]
            df_to_clean_case_2['FyProductNumber'] = df_to_clean_case_2[['FyProductNumber_x']]
            df_to_clean_case_2['VendorName'] = df_to_clean_case_2[['VendorName_x']]
            df_to_clean_case_2['VendorPartNumber'] = df_to_clean_case_2[['VendorPartNumber_x']]

        drop_2 = ['ProductPriceId','BaseProductPriceId','ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_case_2 = df_to_clean_case_2.drop(columns = drop_2)

        if len(df_to_clean_case_3.index) > 0:
            # new vendor for new configuration
            df_to_clean_case_3['ManufacturerName'] = df_to_clean_case_3[['ManufacturerName_x']]
            df_to_clean_case_3['FyProductNumber'] = df_to_clean_case_3[['FyProductNumber_x']]
            df_to_clean_case_3['VendorName'] = df_to_clean_case_3[['VendorName_x']]
            df_to_clean_case_3['VendorPartNumber'] = df_to_clean_case_3[['VendorPartNumber_x']]

        drop_3 = ['ProductPriceId','BaseProductPriceId', 'ManufacturerName_x', 'FyProductNumber_x', 'VendorName_x',
                      'VendorPartNumber_x', 'VendorPartNumber_y', 'VendorName_y', 'FyProductNumber_y', 'ManufacturerName_y']
        df_to_clean_case_3 = df_to_clean_case_3.drop(columns=drop_3)

        if len(df_to_clean_case_4.index) > 0:
            # configuration change
            df_to_clean_case_4['ManufacturerName'] = df_to_clean_case_4[['ManufacturerName_x']]
            df_to_clean_case_4['FyProductNumber'] = df_to_clean_case_4[['FyProductNumber_x']]
            df_to_clean_case_4['CurrentFyProductNumber'] = df_to_clean_case_4[['FyProductNumber_y']]
            df_to_clean_case_4['VendorName'] = df_to_clean_case_4[['VendorName_x']]
            df_to_clean_case_4['VendorPartNumber'] = df_to_clean_case_4[['VendorPartNumber_x']]

        drop_4 = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_case_4 = df_to_clean_case_4.drop(columns = drop_4)

        if len(df_to_clean_case_9.index) > 0:
            # configuration change
            df_to_clean_case_9['ManufacturerName'] = df_to_clean_case_9[['ManufacturerName_y']]
            df_to_clean_case_9['PossibleSupplierName'] = df_to_clean_case_9[['ManufacturerName_x']]
            df_to_clean_case_9['FyProductNumber'] = df_to_clean_case_9[['FyProductNumber_x']]
            df_to_clean_case_9['VendorName'] = df_to_clean_case_9[['VendorName_x']]
            df_to_clean_case_9['VendorPartNumber'] = df_to_clean_case_9[['VendorPartNumber_x']]

        drop_9 = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_to_clean_case_9 = df_to_clean_case_9.drop(columns = drop_9)


        df_partials['ManufacturerName'] = df_partials[['ManufacturerName_x']]
        df_partials['FyProductNumber'] = df_partials[['FyProductNumber_x']]
        df_partials['VendorName'] = df_partials[['VendorName_x']]
        df_partials['VendorPartNumber'] = df_partials[['VendorPartNumber_x']]

        drop_p = ['ManufacturerName_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerName_y']
        df_partials = df_partials.drop(columns = drop_p)

        df_to_return = pandas.concat([df_partials,df_to_clean_case_1,df_to_clean_case_2,df_to_clean_case_3,df_to_clean_case_4,df_to_clean_case_9],ignore_index=True)


        return df_to_return


    def x_y_cleaning(self, df_to_clean):
        #  matched on 'FyCatalogNumber','ManufacturerName'
        df_to_clean_case_5 = df_to_clean[(df_to_clean['Filter'] == 'case_5')].copy()
        df_to_clean_case_6 = df_to_clean[(df_to_clean['Filter'] == 'case_6')].copy()
        df_to_clean_case_7 = df_to_clean[(df_to_clean['Filter'] == 'case_7')].copy()
        df_to_clean_case_8 = df_to_clean[(df_to_clean['Filter'] == 'case_8')].copy()
        df_partials = df_to_clean[(df_to_clean['Filter'] == 'Partial')].copy()

        if len(df_to_clean_case_5.index) > 0:
            # This is a likely override/duplicate
            df_to_clean_case_5['VendorName'] = df_to_clean_case_5[['VendorName_x']]
            df_to_clean_case_5['ManufacturerPartNumber'] = df_to_clean_case_5[['ManufacturerPartNumber_x']]
            df_to_clean_case_5['Other_ManufacturerPartNumber'] = df_to_clean_case_5[['ManufacturerPartNumber_y']]
            df_to_clean_case_5['VendorPartNumber'] = df_to_clean_case_5[['VendorPartNumber_x']]
            df_to_clean_case_5['Other_VendorPartNumber'] = df_to_clean_case_5[['VendorPartNumber_y']]
            df_to_clean_case_5['FyProductNumber'] = df_to_clean_case_5[['FyProductNumber_x']]

        drop_5 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorName_y','VendorPartNumber_x',
                      'VendorPartNumber_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_5 = df_to_clean_case_5.drop(columns = drop_5)

        if len(df_to_clean_case_6.index) > 0:
            #  matched on 'FyCatalogNumber','ManufacturerName'
            # new vendor for existing configuration
            df_to_clean_case_6['FyProductNumber'] = df_to_clean_case_6[['FyProductNumber_x']]
            df_to_clean_case_6['VendorName'] = df_to_clean_case_6[['VendorName_x']]
            df_to_clean_case_6['VendorPartNumber'] = df_to_clean_case_6[['VendorPartNumber_x']]
            df_to_clean_case_6['ManufacturerPartNumber'] = df_to_clean_case_6[['ManufacturerPartNumber_x']]

        drop_6 = ['ProductPriceId','BaseProductPriceId','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_x','ManufacturerPartNumber_y']
        df_to_clean_case_6 = df_to_clean_case_6.drop(columns = drop_6)

        if len(df_to_clean_case_7.index) > 0:
            # configuration change
            df_to_clean_case_7['ManufacturerPartNumber'] = df_to_clean_case_7[['ManufacturerPartNumber_x']]
            df_to_clean_case_7['FyProductNumber'] = df_to_clean_case_7[['FyProductNumber_x']]
            df_to_clean_case_7['CurrentFyProductNumber'] = df_to_clean_case_7[['FyProductNumber_y']]
            df_to_clean_case_7['VendorName'] = df_to_clean_case_7[['VendorName_x']]
            df_to_clean_case_7['VendorPartNumber'] = df_to_clean_case_7[['VendorPartNumber_x']]

        drop_7 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_7 = df_to_clean_case_7.drop(columns = drop_7)

        #  matched on 'FyCatalogNumber','ManufacturerName'
        if len(df_to_clean_case_8.index) > 0:
            df_to_clean_case_8['VendorName'] = df_to_clean_case_8[['VendorName_x']]
            df_to_clean_case_8['ManufacturerPartNumber'] = df_to_clean_case_8[['ManufacturerPartNumber_x']]
            df_to_clean_case_8['VendorName'] = df_to_clean_case_8[['VendorName_x']]
            df_to_clean_case_8['VendorPartNumber'] = df_to_clean_case_8[['VendorPartNumber_x']]
            df_to_clean_case_8['FyProductNumber'] = df_to_clean_case_8[['FyProductNumber_x']]

        drop_8 = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_to_clean_case_8 = df_to_clean_case_8.drop(columns = drop_8)

        df_partials['ManufacturerPartNumber'] = df_partials[['ManufacturerPartNumber_x']]
        df_partials['FyProductNumber'] = df_partials[['FyProductNumber_x']]
        df_partials['VendorName'] = df_partials[['VendorName_x']]
        df_partials['VendorPartNumber'] = df_partials[['VendorPartNumber_x']]

        drop_p = ['ManufacturerPartNumber_x','FyProductNumber_x','VendorName_x','VendorPartNumber_x',
                      'VendorPartNumber_y','VendorName_y','FyProductNumber_y','ManufacturerPartNumber_y']
        df_partials = df_partials.drop(columns = drop_p)

        df_to_return = pandas.concat([df_partials, df_to_clean_case_5,df_to_clean_case_6,df_to_clean_case_7,df_to_clean_case_8],ignore_index=True)

        return df_to_return


    def begin_process(self):
        self.success = False
        if self.is_viable:
            self.success, self.message = self.run_process()
        elif self.message == 'No message':
            missing_heads = self.get_missing_heads()
            missing_string = str(missing_heads)
            missing_string = missing_string.replace(']','')
            missing_string = missing_string.replace('[','')
            missing_string = missing_string.replace('\'','')

            self.df_product['Missing Headers'] = missing_string
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

    def trigger_ingest_cleanup(self):
        pass

    def normalize_units(self, units):
        units = units.upper()

        if units in ['BOTTLE','BOTTLES']:
            units = 'BT'
        elif units in ['BOX','BOXES']:
            units = 'BX'
        elif units in ['CARTON','CARTONS']:
            units = 'CT'
        elif units in ['CASE','CASES']:
            units = 'CS'
        elif units in ['EACH','EACHES','ITEM','ITEMS','TEST','TESTS','TST','TSTS']:
            units = 'EA'
        elif units in ['JAR','JARS']:
            units = 'JR'
        elif units in ['KIT','KITS']:
            units = 'KT'
        elif units in ['PAK','PAKS','PACK','PACKS','PACKAGE','PACKAGES']:
            units = 'PK'
        elif units in ['PAIR','PAIRS']:
            units = 'PR'
        elif units in ['ROLL','ROLLS']:
            units = 'RL'
        elif units in ['SET','SETS']:
            units = 'ST'

        return units

    def row_check(self, row, name_to_check):
        try:
            name_value = row[name_to_check]
            return True, name_value
        except KeyError:
            self.obReporter.update_report('Alert', '{0} was missing.'.format(name_to_check))
            return False, 0


    def float_check(self, float_name_val, report_name):
        try:
            checked_float_value = float(float_name_val)
            if checked_float_value >= 0:
                return True, checked_float_value
            else:
                self.obReporter.update_report('Alert', '{0} must be a positive number.'.format(report_name))
                return False, checked_float_value

        except TypeError:
            self.obReporter.update_report('Alert', '{0} must be a positive number.'.format(report_name))
            return False, 0

        except ValueError:
            return False, float_name_val

    def run_process(self):
        self.obReporter = ReporterObject()
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

            if self.line_viability(df_line_product):
                self.ready_report(df_line_product)
                self.obReporter.report_line_viability(True)

                success, return_df_line_product = self.process_product_line(df_line_product)
                self.obReporter.final_report(success)

            else:
                self.obReporter.report_line_viability(False)
                success, return_df_line_product = self.report_missing_data(df_line_product)

            # appends all the product objects into a list
            report_set = self.obReporter.get_report()
            if 'Pass' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Pass')

            if 'Alert' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Alert')

            if 'Fail' in return_df_line_product.columns:
                return_df_line_product = return_df_line_product.drop(columns='Fail')

            return_df_line_product.insert(1, 'Pass', report_set[0])
            return_df_line_product.insert(2, 'Alert', report_set[1])
            return_df_line_product.insert(3, 'Fail', report_set[2])

            self.obReporter.clear_reports()

            self.collect_return_dfs.append(return_df_line_product)

            if success:
                good += 1
            else:
                bad += 1

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.trigger_ingest_cleanup()

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

    def ready_report(self, df_line_product):
        pass_report = ''
        alert_report = ''
        fail_report = ''
        if 'Pass' in df_line_product.columns:
            pass_report = str(df_line_product['Pass'].values[0])

        if 'Alert' in df_line_product.columns:
            alert_report = str(df_line_product['Alert'].values[0])

        if 'Fail' in df_line_product.columns:
            fail_report = str(df_line_product['Fail'].values[0])

        self.obReporter.set_reports(pass_report,alert_report,fail_report)


    def process_product_line(self, df_line_product):
        self.obReporter.report_no_process()
        return False, df_line_product

    def process_boolean(self, row, isCol):
        try:
            test_val = row[isCol]
        except KeyError:
            self.obReporter.update_report('Alert','{0} missing'.format(isCol))
            return False, isCol

        try:
            test_val = int(test_val)
        except ValueError:
            if test_val.lower() in ['n','no']:
                test_val = 0
            elif test_val.lower() in ['y','yes']:
                test_val = 1

        if test_val not in [0, 1]:
            self.obReporter.update_report('Alert', 'Review {0}'.format(isCol))
            return False, test_val

        return True, test_val

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

        if 'UnitOfIssue' in row:
            unit_of_issue = self.normalize_units(row['UnitOfIssue'])

        else:
            unit_of_issue = 'EA'
            df_collect_product_base_data['UnitOfIssue'] = [unit_of_issue]
            self.obReporter.default_uoi_report()


        if 'ManufacturerId' in row:
            new_manufacturer_id = row['ManufacturerId']
            new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']].values[0][0]

            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix

        if (manufacturer.lower() in self.df_manufacturer_translator['SupplierName'].values):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['SupplierName'] == manufacturer.lower()), ['ManufacturerId',
                                                                                    'FyManufacturerPrefix']].values[0]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix

        elif (manufacturer.upper() in self.df_manufacturer_translator['ManufacturerName'].unique()):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerName'] == manufacturer.upper()), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                0]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix


        elif 'SupplierName' in row:
            supplier = row['SupplierName'].lower()
            if (supplier in self.df_manufacturer_translator['SupplierName'].values):
                new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == supplier), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                    0]

                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

                return True, df_collect_product_base_data, new_prefix
            else:
                manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
                manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))

                new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=supplier, lst_manufacturer_names=manufacturer_name_list)
                self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
                # this needs to return the prefix so it can be used

                new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']]

                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

                return True, df_collect_product_base_data, new_prefix

        else:
            manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
            manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))

            new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=manufacturer, lst_manufacturer_names=manufacturer_name_list)
            self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
            # this needs to return the prefix so it can be used

            new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix



    def process_manufacturer_clean(self, df_collect_product_base_data, row):
        manufacturer = row['ManufacturerName']
        manufacturer = manufacturer.strip().replace('  ',' ')


        if 'ManufacturerId' in row:
            new_manufacturer_id = row['ManufacturerId']
            new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']].values[0][0]

            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix

        if (manufacturer.lower() in self.df_manufacturer_translator['SupplierName'].values):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['SupplierName'] == manufacturer.lower()), ['ManufacturerId',
                                                                                    'FyManufacturerPrefix']].values[0]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix

        elif (manufacturer.upper() in self.df_manufacturer_translator['ManufacturerName'].unique()):
            new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerName'] == manufacturer.upper()), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                0]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix


        elif 'SupplierName' in row:
            supplier = row['SupplierName'].lower()
            if (supplier in self.df_manufacturer_translator['SupplierName'].values):
                new_manufacturer_id, new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['SupplierName'] == supplier), ['ManufacturerId',
                                                                                        'FyManufacturerPrefix']].values[
                    0]

                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

                return True, df_collect_product_base_data, new_prefix
            else:
                manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
                manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))

                new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=supplier, lst_manufacturer_names=manufacturer_name_list)
                self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
                # this needs to return the prefix so it can be used

                new_prefix = self.df_manufacturer_translator.loc[
                    (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']]

                df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
                df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

                return True, df_collect_product_base_data, new_prefix

        else:
            manufacturer_name_list = self.df_manufacturer_translator["ManufacturerName"].tolist()
            manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))

            new_manufacturer_id = self.obIngester.manual_ingest_manufacturer(atmp_sup=manufacturer, lst_manufacturer_names=manufacturer_name_list)
            self.df_manufacturer_translator = self.obIngester.get_manufacturer_lookup()
            # this needs to return the prefix so it can be used

            new_prefix = self.df_manufacturer_translator.loc[
                (self.df_manufacturer_translator['ManufacturerId'] == new_manufacturer_id), ['FyManufacturerPrefix']]

            df_collect_product_base_data['ManufacturerId'] = [new_manufacturer_id]
            df_collect_product_base_data['FyManufacturerPrefix'] = [new_prefix]

            return True, df_collect_product_base_data, new_prefix


    def build_part_number(self, row, manufacturer_part_number, manufacturer_prefix, unit_of_issue, b_override):

        if 'FyCatalogNumber' not in row and 'FyCatalogNumber_y' not in row:
            fy_catalog_number = self.make_fy_catalog_number(manufacturer_prefix, manufacturer_part_number, b_override)

        elif 'FyCatalogNumber' in row:
            fy_catalog_number = row['FyCatalogNumber']

        elif 'FyCatalogNumber_y' in row:
            fy_catalog_number = row['FyCatalogNumber_y']


        if 'FyProductNumber' not in row and 'FyProductNumber_y' not in row :

            fy_product_number = fy_catalog_number

            if unit_of_issue != 'EA':
                if fy_catalog_number[:-2] == unit_of_issue:
                    self.obReporter.update_report('Alert', 'Please check for duplicate units in FyProductNumber')
                fy_product_number = fy_catalog_number + ' ' + unit_of_issue

        elif 'FyProductNumber' in row:
            fy_product_number = row['FyProductNumber']

        elif 'FyProductNumber_y' in row:
            fy_product_number = row['FyProductNumber_y']

        return fy_catalog_number, fy_product_number


    def make_fy_catalog_number(self,prefix, manufacturer_part_number, b_override = False):
        if b_override:
            if len(manufacturer_part_number) >= 22:
                self.obReporter.update_report('Alert','Long Manufacturer Part Number')

            FY_catalog_number = str(prefix) + '-' + manufacturer_part_number.upper()
        else:
            clean_part_number = self.obValidator.clean_part_number(manufacturer_part_number)
            FY_catalog_number = str(prefix)+'-'+clean_part_number.upper()

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
        report = 'Missing Data: ' + str(missing_headers)[1:-1]+'.'
        report = report.replace("\'",'')

        self.obReporter.update_report('Fail',report)
        return False, df_line_product


    def get_df(self):
        return self.df_product



class ReporterObject():
    def __init__(self):
        self.name = 'Lois Lane'
        self.fail_report = ''
        self.alert_report = ''
        self.pass_report = ''

    def update_report(self, report_type, report_text):
        report_types_allowed = ['Fail','Alert','Pass']
        if report_type == 'Fail':
            if report_text not in self.fail_report:
                if self.fail_report != '':
                    self.fail_report = self.fail_report+'; '+report_text
                else:
                    self.fail_report = report_text

        if report_type == 'Alert':
            if report_text not in self.alert_report:
                if self.alert_report != '':
                    self.alert_report = self.alert_report+'; '+report_text
                else:
                    self.alert_report = report_text

        if report_type == 'Pass':
            if report_text not in self.pass_report:
                if self.pass_report != '':
                    self.pass_report = self.pass_report+'; '+report_text
                else:
                    self.pass_report = report_text


    def get_report(self):
        return self.pass_report, self.alert_report, self.fail_report

    def set_reports(self,pass_report,alert_report,fail_report):
        self.fail_report = fail_report
        self.alert_report = alert_report
        self.pass_report = pass_report

    def clear_reports(self):
        self.fail_report = ''
        self.alert_report = ''
        self.pass_report = ''

    def report_no_process(self):
        self.update_report('Alert', 'No process built')

    def report_line_viability(self,is_good):
        if is_good:
            self.update_report('Pass', 'Passed Line Viability')
        else:
            self.update_report('Fail', 'Failed Line Viability')

    def final_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Success at exit')
        else:
            self.update_report('Fail', 'Failed at exit')

    def report_new_manufacturer(self):
        self.update_report('Fail', 'Manufacturer must be ingested')

    def price_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Minumum product price success')
        else:
            self.update_report('Fail', 'Minumum product price failure')


    def fill_price_report(self,is_good):
        if is_good:
            self.update_report('Pass', 'Fill product price success')
        else:
            self.update_report('Fail', 'Fill product price failure')


    def default_uoi_report(self):
        self.update_report('Alert', 'Default UOI')




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




