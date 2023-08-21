# CreatedBy: Emilia Crow
# CreateDate: 20210527
# Updated: 20220318
# CreateFor: Franklin Young International

import pandas

from Tools.ProgressBar import YesNoDialog
from Tools.BasicProcess import BasicProcessObject

# keep this
class MinimumProduct(BasicProcessObject):
    req_fields = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'FyProductNumber', 'VendorName',
                  'VendorPartNumber']
    sup_fields = []
    att_fields = ['RecommendedStorage', 'Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = ['ManufacturerId', 'FyManufacturerPrefix', 'IsFreeShipping', 'IsColdChain',
                  'ShippingInstructionsId', 'RecommendedStorageId', 'ExpectedLeadTimeId']


    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Minimum Product'
        self.previous_fy_catalog_number = -1


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new(b_match_vendor = True)
        self.batch_process_category()

        if 'RecommendedStorage' not in self.df_product.columns:
            self.df_product['RecommendedStorageId'] = '1'
        else:
            self.df_product['RecommendedStorage'].replace(to_replace = '',value='No storage info.',inplace=True)
            self.batch_process_attribute('RecommendedStorage')


        self.batch_process_lead_time()

        self.df_product.sort_values(by=['FyCatalogNumber'], inplace=True)

        self.inserted_products = []

        return self.df_product

    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'db_IsProductNumberOverride','Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product')
            return True

        elif row['Filter'] in ['Partial', 'Base Pricing','ConfigurationChange','check_vendor']:
            self.obReporter.update_report('Alert', 'Passed filtering as new configuration')
            return True

        elif row['Filter'] == 'Ready':
            self.obReporter.update_report('Alert', 'Passed filtering as ready')
            return False

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Fail', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False


    def batch_process_category(self):

        # this needs to be handled better
        if 'Category' in self.df_product.columns:
            df_attribute = self.df_product[['Category']]
            df_attribute = df_attribute.drop_duplicates(subset=['Category'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                category = str(row['Category']).strip()
                if category == '':
                    new_category_id = -1
                    lst_ids.append(new_category_id)
                    continue

                while '/ ' in category:
                    category = category.replace('/ ', '/')

                while ' /' in category:
                    category = category.replace(' /', '/')

                category_name = (category.rpartition('/')[2]).strip()

                self.df_category_names['CategoryNameLower'] = self.df_category_names['CategoryName'].str.lower()

                if category_name in self.df_category_names['CategoryName'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]

                elif category_name.lower() in self.df_category_names['CategoryNameLower'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryNameLower'] == category_name.lower()), 'CategoryId'].values[0]

                elif category in self.df_category_names['Category'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['Category'] == category), 'CategoryId'].values[0]

                else:
                    categories_to_ship = []
                    if (category != '') and (category_name != '') and ('All Products/' in category):
                        # collect the biggest one
                        categories_to_ship.append([category_name, category])

                        # we set the name of the category
                        category = category.rpartition('/')[0]
                        category_name = category.rpartition('/')[2]

                        # as long as the hierarchy exists, we split it out
                        while ('/' in category):
                            category_name = category_name.strip()
                            category = category.strip()

                            if category_name in self.df_category_names['CategoryName'].values:
                                break
                            elif category in self.df_category_names['Category'].values:
                                break

                            categories_to_ship.append([category_name, category])

                            # we set the name of the category
                            category = category.rpartition('/')[0]
                            category_name = category.rpartition('/')[2]

                        # this is the magic
                        # this sets the order smallest to largest
                        # this puts the mapping into the DB in the right order
                        # and returns the correct id at the end
                        categories_to_ship.sort(key=lambda x:len(x[1]))

                        for each_category in categories_to_ship:
                            new_category_id = self.obDal.category_cap(each_category[0], each_category[1])

                lst_ids.append(new_category_id)

            df_attribute['CategoryId'] = lst_ids

            self.df_product = self.df_product.merge(df_attribute,
                                                     how='left', on=['Category'])
        else:
            self.df_fill_category = self.obDal.get_product_category()
            self.df_product = self.df_product.merge(self.df_fill_category,how='left',on=['ProductId'])
            del self.df_fill_category


    def batch_process_lead_time(self):
        if 'ExpectedLeadTimeId' not in self.df_product.columns:
            self.df_lead_times = self.obDal.get_lead_times()

            # adjust lead times to only handle lead times
            # convert to number of days

            if 'LeadTime' in self.df_product.columns:
                df_attribute = self.df_product[['LeadTime']]
                df_attribute = df_attribute.drop_duplicates(subset=['LeadTime'])
                lst_ids = []
                for colName, row in df_attribute.iterrows():
                    lead_time = row['LeadTime']
                    success, lead_time = self.float_check(lead_time, 'lead time')
                    if success:
                        lead_time = int(lead_time)
                        try:
                            new_lead_time_id = self.df_lead_times.loc[
                                (self.df_lead_times['LeadTime'] == lead_time), 'ExpectedLeadTimeId'].values[0]
                        except IndexError:
                            if 'LeadTimeExpedited' in row:
                                success, expedited_lead_time = self.float_check(row['LeadTimeExpedited'], 'Lead Time Expedited')
                                if not success:
                                    new_lead_time_id = -1

                            else:
                                expedited_lead_time = lead_time
                            new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)

                    elif lead_time != '':
                        if 'day' in lead_time:
                            lead_time = int(lead_time.rpartition('day')[0])

                        elif 'week' in lead_time:
                            lead_time = int(lead_time.rpartition('week')[0]) * 7

                        elif 'month' in lead_time:
                            lead_time = int(lead_time.rpartition('month')[0]) * 30

                        elif 'year' in lead_time:
                            lead_time = int(lead_time.rpartition('year')[0]) * 365

                        else:
                            new_lead_time_id = -1


                        if 'LeadTimeExpedited' in row:
                            expedited_lead_time = row['LeadTimeExpedited']
                        else:
                            expedited_lead_time = lead_time


                        if lead_time in self.df_lead_times['LeadTime'].tolist():
                            new_lead_time_id = self.df_lead_times.loc[
                                (self.df_lead_times['LeadTime'] == lead_time), 'ExpectedLeadTimeId'].values[0]

                        else:
                            new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)

                    else:
                        new_lead_time_id = -1


                    lst_ids.append(new_lead_time_id)

                df_attribute['ExpectedLeadTimeId'] = lst_ids
                self.df_product = self.df_product.merge(df_attribute,how='left', on=['LeadTime'])

            elif 'FyLeadTimes' in self.df_product.columns:
                df_attribute = self.df_product[['FyLeadTimes']]
                df_attribute = df_attribute.drop_duplicates(subset=['FyLeadTimes'])
                lst_ids = []
                for colName, row in df_attribute.iterrows():
                    lead_time = row['FyLeadTimes']
                    success, lead_time = self.float_check(lead_time, 'lead time')
                    if success:
                        lead_time = int(lead_time)
                        try:
                            new_lead_time_id = self.df_lead_times.loc[
                                (self.df_lead_times['LeadTime'] == lead_time), 'ExpectedLeadTimeId'].values[0]
                        except IndexError:
                            if 'LeadTimeExpedited' in row:
                                success, expedited_lead_time = self.float_check(row['LeadTimeExpedited'], 'Lead Time Expedited')
                                if not success:
                                    new_lead_time_id = -1

                            else:
                                expedited_lead_time = lead_time
                            new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)

                    elif lead_time != '':
                        if 'day' in lead_time:
                            lead_time = int(lead_time.rpartition('day')[0])

                        elif 'week' in lead_time:
                            lead_time = int(lead_time.rpartition('week')[0]) * 7

                        elif 'month' in lead_time:
                            lead_time = int(lead_time.rpartition('month')[0]) * 30

                        elif 'year' in lead_time:
                            lead_time = int(lead_time.rpartition('year')[0]) * 365

                        else:
                            new_lead_time_id = -1


                        if 'LeadTimeExpedited' in row:
                            expedited_lead_time = row['LeadTimeExpedited']
                        else:
                            expedited_lead_time = lead_time


                        if lead_time in self.df_lead_times['LeadTime'].tolist():
                            new_lead_time_id = self.df_lead_times.loc[
                                (self.df_lead_times['LeadTime'] == lead_time), 'ExpectedLeadTimeId'].values[0]

                        else:
                            new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)

                    else:
                        new_lead_time_id = -1


                    lst_ids.append(new_lead_time_id)

                df_attribute['ExpectedLeadTimeId'] = lst_ids
                self.df_product = self.df_product.merge(df_attribute,how='left', on=['FyLeadTimes'])
            else:
                self.df_product['ExpectedLeadTimeId'] = -1


    def batch_process_attribute(self, attribute):
        set_128 = ['RecommendedStorageId']
        str_attribute_id = attribute +'Id'
        if str_attribute_id not in self.df_product.columns:
            df_attribute = self.df_product[[attribute]]
            df_attribute = df_attribute.drop_duplicates(subset=[attribute])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                attribute_phrase = row[attribute]
                if attribute_phrase in set_128:
                    attribute_phrase = attribute_phrase[:128]

                attribute_id = self.obIngester.ingest_attribute(attribute_phrase, attribute)
                lst_ids.append(attribute_id)

            df_attribute[str_attribute_id] = lst_ids

            self.df_product = self.df_product.merge(df_attribute,
                                                              how='left', on=[attribute])


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        df_line_product = self.process_attribute_data(df_collect_product_base_data)
        df_collect_product_base_data = df_line_product.copy()


        # this is also stupid, but it gets the point across for testing purposes
        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            if 'FyCatalogNumber' not in row:
                success, df_collect_product_base_data, fy_manufacturer_prefix = self.process_manufacturer(df_collect_product_base_data, row)
                if not success:
                    self.obReporter.report_new_manufacturer()
                    return success, df_collect_attribute_data

            if ('CategoryId' not in row):
                self.obReporter.update_report('Alert','No category assigned.')

            if ('VendorId' in row):
                vendor_id = row['VendorId']
                if vendor_id == -1:
                    self.obReporter.update_report('Fail','Bad vendor name.')
                    return False, df_collect_product_base_data

            if ('ShippingInstructionsId' not in row):
                success, df_collect_product_base_data = self.process_shipping(df_collect_product_base_data, row)
                if success == False:
                    self.obReporter.update_report('Fail','Failed in process shipping instructions')
                    return success, df_collect_product_base_data

            if ('ExpectedLeadTimeId' not in row):
                self.obReporter.update_report('Fail','Failed in process lead time')
                return success, df_collect_product_base_data

            if ('RecommendedStorageId' not in row):
                self.obReporter.update_report('Fail','Failed in process recommended storage')
                return False, df_collect_product_base_data


            b_override = False
            success, return_val = self.process_boolean(row, 'FyProductNumberOverride')
            if success and return_val == 1:
                b_override = True
            else:
                success, return_val = self.process_boolean(row, 'db_ProductNumberOverride')
                if success and return_val == 1:
                    b_override = True

            fy_catalog_number = row['FyCatalogNumber']
            b_pass_number_check = self.obValidator.review_product_number(fy_catalog_number)
            if not b_pass_number_check:
                if not b_override:
                    self.obReporter.update_report('Fail', 'Your catalog number contains outlawed characters')
                    return False, df_collect_product_base_data

        if self.previous_fy_catalog_number != fy_catalog_number:
            return_df_line_product = self.minimum_product(df_collect_product_base_data)
        else:
            self.obReporter.update_report('Alert', 'This product was skipped as a repeat ingestion')
            return True, df_collect_product_base_data

        self.previous_fy_catalog_number = fy_catalog_number

        return True, return_df_line_product


    def process_shipping(self, df_collect_product_base_data, row):
        shipping_desc = 'No shipping instructions.'
        shipping_code = ''
        is_free_shipping = 0
        is_cold_chain = 0

        if 'ShippingInstructions' in row:
            shipping_desc = row['ShippingInstructions']

        if ('ShippingCode' in row):
            shipping_code = row['ShippingCode']

        for each_bool in ['IsFreeShipping','IsColdChain']:
            success, return_val = self.process_boolean(row, each_bool)
            if success:
                df_collect_product_base_data[each_bool] = [return_val]
            else:
                self.obReporter.update_report('Alert', '{0} was set to 0'.format(each_bool))
                df_collect_product_base_data[each_bool] = [0]


        # todo: This needs to pull in the begining and check a DF here
        # I've added a cheap fix for now, but this is a lazy solution you'll have to deal with later. I know, I'm a jerk.
        if [shipping_desc, shipping_code, is_free_shipping, is_cold_chain] != ['No shipping instructions.','',0,0]:
            df_collect_product_base_data['ShippingInstructionsId'] = self.obIngester.ingest_shipping_instructions(
                shipping_desc, shipping_code, is_free_shipping, is_cold_chain)
        else:
            df_collect_product_base_data['ShippingInstructionsId'] = 1

        return True, df_collect_product_base_data


    def minimum_product(self,df_line_product):
        # here all processing and checks have been done
        # we just get data from the DF and ship it (as planned)
        for colName, row in df_line_product.iterrows():
            fy_catalog_number = row['FyCatalogNumber']
            manufacturer_part_number = row['ManufacturerPartNumber']

            b_override = 0
            success, return_val = self.process_boolean(row, 'FyProductNumberOverride')
            if success and return_val == 1:
                b_override = 1

            success, return_val = self.process_boolean(row, 'db_ProductNumberOverride')
            if success and return_val == 1:
                b_override = 1

            manufacturer_id = row['ManufacturerId']
            try:
                category_id = int(row['CategoryId'])
            except KeyError:
                category_id = -1

            shipping_instructions_id = row['ShippingInstructionsId']
            recommended_storage_id = row['RecommendedStorageId']
            expected_lead_time_id = int(row['ExpectedLeadTimeId'])

        if str(row['Filter']) == 'New':
            if (expected_lead_time_id == -1):
                self.obReporter.update_report('Fail','Missing LeadTime')
                return df_line_product

            else:
                if fy_catalog_number not in self.inserted_products:
                    self.obIngester.insert_product(fy_catalog_number, manufacturer_part_number, b_override, manufacturer_id,
                                                 shipping_instructions_id, recommended_storage_id,
                                                 expected_lead_time_id, category_id)
                    self.inserted_products.append(fy_catalog_number)
                else:
                    print('Skippi product {0}'.format(fy_catalog_number))

        elif str(row['Filter']) == 'Ready' or str(row['Filter']) == 'Partial' or str(row['Filter']) == 'Base Pricing':
            product_id = row['ProductId']
            self.obIngester.update_product(product_id, fy_catalog_number, manufacturer_part_number, b_override, manufacturer_id,
                                                 shipping_instructions_id, recommended_storage_id,
                                                 expected_lead_time_id, category_id)

        return df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_product_cleanup()
        self.obIngester.update_product_cleanup()



class UpdateMinimumProduct(MinimumProduct):
    req_fields = ['FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber', 'FyProductNumber', 'VendorName','VendorPartNumber']
    sup_fields = []
    att_fields = ['RecommendedStorage', 'Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = ['ManufacturerId', 'FyManufacturerPrefix', 'FyCatalogNumber',
                                'IsFreeShipping', 'IsColdChain', 'ShippingInstructionsId', 'RecommendedStorageId',
                                'ExpectedLeadTimeId']

    def __init__(self,df_product, user, password, is_testing, full_process=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Minimum Product'
        self.full_process = full_process


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            if self.full_process:
                self.obReporter.update_report('Alert', 'Passed filtering as a new product')
                return True
            else:
                self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
                return False

        elif row['Filter'] in ['Ready', 'Partial','Base Pricing','ConfigurationChange','check_vendor']:
            self.obReporter.update_report('Alert', 'Passed filtering as updatable')
            return True

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Fail', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False



## end ##
