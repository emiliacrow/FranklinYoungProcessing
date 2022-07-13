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
    gen_fields = ['CountryOfOriginId', 'ManufacturerId', 'FyManufacturerPrefix', 'IsFreeShipping', 'IsColdChain',
                  'ShippingInstructionsId', 'RecommendedStorageId', 'ExpectedLeadTimeId']


    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Minimum Product'
        self.quick_country = {}
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

        if 'CountryOfOrigin' not in self.df_product.columns:
            self.df_product['CountryOfOrigin'] = 'UNKNOWN'
        else:
            self.df_product['CountryOfOrigin'].replace(to_replace = '',value='UNKNOWN',inplace=True)

        self.batch_process_country()
        self.batch_process_lead_time()

        self.df_product.sort_values(by=['FyCatalogNumber'], inplace=True)

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

        elif row['Filter'] in ['Partial', 'Base Pricing']:
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
                category_name = (category.rpartition('/')[2]).strip()

                category_name = category_name.replace('/ ', '/')
                category_name = category_name.replace(' /', '/')

                category_name = category_name.strip()
                category = category.strip()

                if category_name in self.df_category_names['CategoryName'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]

                elif category in self.df_category_names['Category'].values:
                    new_category_id = self.df_category_names.loc[
                        (self.df_category_names['Category'] == category), 'CategoryId'].values[0]
                else:
                    categories_to_ship = []
                    if (category != '') and (category_name != '') and ('All Products/' in category_name):

                        # we set the name of the category
                        new_category = category_name.rpartition('/')[2]
                        # we strip whitespace
                        # collect the biggest one
                        categories_to_ship.append([new_category, category_name])

                        # as long as the hierarchy exists, we split it out
                        while ('/' in category_name):
                            category_name = category_name.rpartition('/')[0]
                            new_category = category_name.rpartition('/')[2]
                            category_name = category_name.strip()
                            new_category = new_category.strip()
                            categories_to_ship.append([new_category, category_name])

                        # this is the magic
                        # this sets the order smallest to largest
                        # this puts the mapping into the DB in the right order
                        # and returns the correct id at the end
                        categories_to_ship.sort(key=lambda x:len(x[1]))

                        # ship it!
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


    def batch_process_country(self):
        if 'CountryOfOriginId' not in self.df_product.columns and 'CountryOfOrigin' in self.df_product.columns:

            df_attribute = self.df_product[['CountryOfOrigin']]
            df_attribute = df_attribute.drop_duplicates(subset=['CountryOfOrigin'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                country = row['CountryOfOrigin']
                country = self.obValidator.clean_country_name(country)
                country = country.upper()
                if (len(country) == 2):
                    if country in self.df_country_translator['CountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    elif country in ['XX','ZZ']:
                        # unknown
                        lst_ids.append(-1)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(atmp_code = country)
                        lst_ids.append(coo_id)

                elif (len(country) == 3):
                    if country in self.df_country_translator['ECATCountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['ECATCountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(ecat_code = country)
                        lst_ids.append(coo_id)

                elif (len(country) > 3):
                    if country in self.df_country_translator['CountryName'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryName'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(atmp_name = country)
                        lst_ids.append(coo_id)
                else:
                    lst_ids.append(-1)

            df_attribute['CountryOfOriginId'] = lst_ids
            self.df_product = self.df_product.merge(df_attribute,
                                                              how='left', on=['CountryOfOrigin'])

        if 'CountryOfOriginId' not in self.df_product.columns and 'FyCountryOfOrigin' in self.df_product.columns:

            df_attribute = self.df_product[['FyCountryOfOrigin']]
            df_attribute = df_attribute.drop_duplicates(subset=['FyCountryOfOrigin'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                country = row['FyCountryOfOrigin']
                country = self.obValidator.clean_country_name(country)
                country = country.upper()
                if (len(country) == 2):
                    if country in self.df_country_translator['CountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    elif country in ['XX','ZZ']:
                        # unknown
                        lst_ids.append(-1)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(atmp_code = country)
                        lst_ids.append(coo_id)

                elif (len(country) == 3):
                    if country in self.df_country_translator['ECATCountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['ECATCountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(ecat_code = country)
                        lst_ids.append(coo_id)

                elif (len(country) > 3):
                    if country in self.df_country_translator['CountryName'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryName'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        coo_id = self.obIngester.manual_ingest_country(atmp_name = country)
                        lst_ids.append(coo_id)
                else:
                    lst_ids.append(-1)

            df_attribute['CountryOfOriginId'] = lst_ids
            self.df_product = self.df_product.merge(df_attribute,
                                                              how='left', on=['FyCountryOfOrigin'])


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

            elif 'FyLeadTime' in self.df_product.columns:
                df_attribute = self.df_product[['FyLeadTime']]
                df_attribute = df_attribute.drop_duplicates(subset=['FyLeadTime'])
                lst_ids = []
                for colName, row in df_attribute.iterrows():
                    lead_time = row['FyLeadTime']
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


                        if lead_time in self.df_lead_times['FyLeadTime'].tolist():
                            new_lead_time_id = self.df_lead_times.loc[
                                (self.df_lead_times['FyLeadTime'] == lead_time), 'ExpectedLeadTimeId'].values[0]

                        else:
                            new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)

                    else:
                        new_lead_time_id = -1


                    lst_ids.append(new_lead_time_id)

                df_attribute['ExpectedLeadTimeId'] = lst_ids
                self.df_product = self.df_product.merge(df_attribute,how='left', on=['FyLeadTime'])
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

            success, df_collect_product_base_data = self.process_long_desc(df_collect_product_base_data, row)
            if success == False:
                self.obReporter.update_report('Fail','Failed in process long description')
                return success, df_collect_product_base_data

            if ('CountryOfOriginId' not in row):
                self.obReporter.update_report('Fail','Failed in process country of origin')
                return False, df_collect_product_base_data
            elif (row['CountryOfOriginId'] == 259):
                self.obReporter.update_report('Alert','Country of origin unknown')

            if ('CategoryId' not in row):
                self.obReporter.update_report('Alert','No category assigned.')

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


    def process_long_desc(self, df_collect_product_base_data, row):
        product_description = ''
        fy_product_description = ''
        product_name = ''
        fy_product_name = ''
        if 'ProductDescription' in row:
            product_description = str(row['ProductDescription'])
            product_description = self.obValidator.clean_description(product_description)
            df_collect_product_base_data['ProductDescription'] = [product_description]
        if 'FyProductDescription' in row:
            fy_product_description = str(row['FyProductDescription'])
            fy_product_description = self.obValidator.clean_description(fy_product_description)
            df_collect_product_base_data['FyProductDescription'] = [fy_product_description]
        if product_description == '' and fy_product_description != '':
            product_description = fy_product_description
            df_collect_product_base_data['ProductDescription'] = [product_description]

        if 'ProductName' in row:
            product_name = str(row['ProductName'])
            product_name = self.obValidator.clean_description(product_name)
            df_collect_product_base_data['ProductName'] = [product_name]
        if 'FyProductName' in row:
            fy_product_name = str(row['FyProductName'])
            fy_product_name = self.obValidator.clean_description(fy_product_name)
            df_collect_product_base_data['FyProductName'] = [fy_product_name]
        if product_name == '' and fy_product_name != '':
            fy_product_name = fy_product_name
            df_collect_product_base_data['ProductName'] = [fy_product_name]


        # processing/cleaning
        if 'LongDescription' in row:
            long_desc = str(row['LongDescription'])
            long_desc = self.obValidator.clean_description(long_desc)
            df_collect_product_base_data['LongDescription'] = [long_desc]
        else:
            long_desc = ''

        if 'ECommerceLongDescription' in row:
            ec_long_desc = str(row['ECommerceLongDescription'])
            ec_long_desc = self.obValidator.clean_description(ec_long_desc)
            df_collect_product_base_data['ECommerceLongDescription'] = [ec_long_desc]
        else:
            ec_long_desc = ''

        if len(ec_long_desc) > 700:
            ec_long_desc = ec_long_desc[:700]

        df_collect_product_base_data['ECommerceLongDescription'] = [ec_long_desc]
        df_collect_product_base_data['LongDescription'] = [long_desc]

        return True, df_collect_product_base_data


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

            try:
                product_name = row['ProductName']
                product_name = self.obValidator.clean_description(product_name)
            except:
                product_name = ''

            try:
                product_description = row['ProductDescription']
                product_description = self.obValidator.clean_description(product_description)
            except:
                product_description = ''

            try:
                long_desc = row['LongDescription']
                long_desc = self.obValidator.clean_description(long_desc)
            except:
                long_desc = ''

            try:
                ec_long_desc = row['ECommerceLongDescription']
                ec_long_desc = self.obValidator.clean_description(ec_long_desc)
            except:
                ec_long_desc = ''

            country_of_origin_id = row['CountryOfOriginId']

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
            if (country_of_origin_id == -1):
                self.obReporter.update_report('Fail','Missing CountryOfOrigin')
                return df_line_product

            if (product_name == ''):
                self.obReporter.update_report('Fail','Missing ProductName')
                return df_line_product
            if (product_description == ''):
                self.obReporter.update_report('Fail','Missing ProductDescription')
                return df_line_product
            else:
                self.obIngester.insert_product(fy_catalog_number, manufacturer_part_number, b_override, product_name, product_description,
                                                 long_desc, ec_long_desc, country_of_origin_id, manufacturer_id,
                                                 shipping_instructions_id, recommended_storage_id,
                                                 expected_lead_time_id, category_id)

        elif str(row['Filter']) == 'Ready' or str(row['Filter']) == 'Partial' or str(row['Filter']) == 'Base Pricing':
            product_id = row['ProductId']
            self.obIngester.update_product(product_id, fy_catalog_number, manufacturer_part_number, b_override, product_name, product_description,
                                                 long_desc, ec_long_desc, country_of_origin_id, manufacturer_id,
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
    gen_fields = ['CountryOfOriginId', 'ManufacturerId', 'FyManufacturerPrefix', 'FyCatalogNumber',
                                'IsFreeShipping', 'IsColdChain', 'ShippingInstructionsId', 'RecommendedStorageId',
                                'ExpectedLeadTimeId']

    def __init__(self,df_product, user, password, is_testing, full_process=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Minimum Product'
        self.quick_country = {}
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

        elif row['Filter'] in ['Ready', 'Partial','Base Pricing']:
            self.obReporter.update_report('Alert', 'Passed filtering as updatable')
            return True

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Fail', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False



## end ##
