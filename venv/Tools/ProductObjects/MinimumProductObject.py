# CreatedBy: Emilia Crow
# CreateDate: 20210527
# Updated: 20210528
# CreateFor: Franklin Young International

import pandas
from Tools.BasicProcess import BasicProcessObject


# keep this
class MinimumProduct(BasicProcessObject):
    req_fields = ['ProductName', 'ShortDescription', 'ManufacturerPartNumber',
                                'CountryOfOrigin', 'ManufacturerName','VendorName','Category']
    sup_fields = []
    att_fields = ['RecommendedStorage', 'Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = ['CountryOfOriginId', 'ManufacturerId', 'FyManufacturerPrefix', 'FyCatalogNumber',
                                'IsFreeShipping', 'IsColdChain', 'ShippingInstructionsId', 'RecommendedStorageId',
                                'ExpectedLeadTimeId']

    def __init__(self,df_product,is_testing):
        super().__init__(df_product,is_testing)
        self.name = 'Minimum Product'
        self.quick_country = {}


    def batch_preprocessing(self):
        # TODO add something here to allow the user to just select the vendor name.
        self.batch_process_vendor()
        self.define_new()
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

        return self.df_product


    def batch_process_vendor(self):
        # there should only be one vendor, really.
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
                new_vendor_id = -1

            lst_ids.append(new_vendor_id)

        df_attribute['VendorId'] = lst_ids

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])

    def define_new(self):
        self.df_loaded_product = self.obDal.get_product_lookup()
        self.df_loaded_product['Filter'] = 'Update'
        self.df_loaded_product['ManufacturerPartNumber'].astype(str)
        self.df_product = self.df_product.merge(self.df_loaded_product,how='left',on=['FyCatalogNumber','ManufacturerPartNumber'])
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'New'


        # at the end of this there must be two df's
        # an update and a new
        # the update will be split based on what needs to update


    def batch_process_category(self):
        df_attribute = self.df_product[['Category']]
        df_attribute = df_attribute.drop_duplicates(subset=['Category'])
        lst_ids = []
        for colName, row in df_attribute.iterrows():
            category = row['Category']
            category_name = category.rpartition('/')[2]
            if category_name in self.df_category_names['CategoryName'].values:
                new_category_id = self.df_category_names.loc[
                    (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]

            elif category in self.df_category_names['Category'].values:
                new_category_id = self.df_category_names.loc[
                    (self.df_category_names['Category'] == category), 'CategoryId'].values[0]
            else:
                # this might be causing a slow down
                # review this to see if it's working right, it should be
                new_category_id = self.obDal.category_cap(category_name, category)

            lst_ids.append(new_category_id)

        df_attribute['CategoryId'] = lst_ids

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['Category'])


    def batch_process_country(self):
        if 'CountryOfOriginId' not in self.df_product.columns:

            df_attribute = self.df_product[['CountryOfOrigin']]
            df_attribute = df_attribute.drop_duplicates(subset=['CountryOfOrigin'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                country = row['CountryOfOrigin']
                country = self.obValidator.clean_part_number(country)
                country = country.upper()
                if (len(country) == 2):
                    if country in self.df_country_translator['CountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        print(self.df_country_translator['CountryCode'])
                        print('-'+country+'-')
                        x = input('1 Bad country')


                elif (len(country) == 3):
                    if country in self.df_country_translator['ECATCountryCode'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['ECATCountryCode'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        print('-'+country+'-')
                        x = input('2 Bad country')

                elif (len(country) > 3):
                    if country in self.df_country_translator['CountryName'].tolist():
                        new_country_of_origin_id = self.df_country_translator.loc[
                            (self.df_country_translator['CountryName'] == country), 'CountryOfOriginId'].values[0]
                        lst_ids.append(new_country_of_origin_id)
                    else:
                        print('-'+country+'-')
                        x = input('3 Bad country')
                else:
                    lst_ids.append(259)

            df_attribute['CountryOfOriginId'] = lst_ids
            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                              how='left', on=['CountryOfOrigin'])


    def batch_process_lead_time(self):
        self.df_lead_times = self.obDal.get_lead_times()

        if 'ExpectedLeadTime' not in self.df_product.columns:
            self.df_product['ExpectedLeadTimeId'] = 2

        else:
            df_attribute = self.df_product[['LeadTimeDays']]
            df_attribute = df_attribute.drop_duplicates(subset=['LeadTimeDays'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                lead_time = row['LeadTimeDays']
                if lead_time in self.df_lead_times['LeadTimeDays'].tolist():
                    new_lead_time_id = self.df_lead_times.loc[
                        (self.df_lead_times['LeadTimeDays'] == lead_time), 'ExpectedLeadTimeId'].values[0]

                else:
                    if 'LeadTimeDaysExpedited' in row:
                        expedited_lead_time = row['LeadTimeDaysExpedited']
                    else:
                        expedited_lead_time = lead_time

                    new_lead_time_id = self.obIngester.ingest_expected_lead_times(lead_time, expedited_lead_time)
                lst_ids.append(new_lead_time_id)

            df_attribute['ExpectedLeadTimeId'] = lst_ids
            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                              how='left', on=['ExpectedLeadTime'])

    def batch_process_attribute(self,attribute):
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

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                              how='left', on=[attribute])

    def process_product_line(self, df_line_product):
        is_controlled = 0
        is_disposible = 0
        is_green = 0
        is_latex_free = 0
        is_rx = 0

        success = True
        df_collect_product_base_data = df_line_product.copy()

        df_collect_product_base_data = self.process_attribute_data(df_collect_product_base_data)

        df_line_product = df_collect_product_base_data.copy()
        # this is also stupid, but it gets the point across for testing purposes
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Update':
                    df_collect_product_base_data['FinalReport'] = ['This product was not new']
                    return True, df_collect_product_base_data

            success, df_collect_product_base_data = self.process_long_desc(df_collect_product_base_data, row)
            if success == False:
                df_collect_product_base_data['FinalReport'] = ['Failed in process long description']
                return success, df_collect_product_base_data

            if ('CountryOfOriginId' not in row):
                df_collect_product_base_data['FinalReport'] = ['Failed in process country of origin']
                return False, df_collect_product_base_data

            if ('ManufacturerId' not in row):
                success, df_collect_product_base_data = self.process_manufacturer(df_collect_product_base_data, row)
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['Failed in process manufacturer']
                    return success, df_collect_product_base_data

            if ('ShippingInstructionsId' not in row):
                success, df_collect_product_base_data = self.process_shipping(df_collect_product_base_data, row)
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['Failed in process shipping instructions']
                    return success, df_collect_product_base_data

            if ('ExpectedLeadTimeId' not in row):
                df_collect_product_base_data['FinalReport'] = ['Failed in process lead time']
                return success, df_collect_product_base_data

            if ('RecommendedStorageId' not in row):
                df_collect_product_base_data['FinalReport'] = ['Failed in process recommended storage']
                return False, df_collect_product_base_data

            if 'IsControlled' in row:
                success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row,
                                                                             'IsControlled')
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['IsControlled failed boolean evaluation']
                    return success, df_collect_product_base_data
            else:
                df_collect_product_base_data['IsControlled'] = [is_controlled]

            if 'IsDisposable' in row:
                success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row,
                                                                             'IsDisposable')
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['IsDisposable failed boolean evaluation']
                    return success, df_collect_product_base_data
            else:
                df_collect_product_base_data['IsDisposable'] = [is_disposible]

            if 'IsGreen' in row:
                success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row, 'IsGreen')
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['IsGreen failed boolean evaluation']
                    return success, df_collect_product_base_data
            else:
                df_collect_product_base_data['IsGreen'] = [is_green]

            if 'IsLatexFree' in row:
                success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row,
                                                                             'IsLatexFree')
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['IsLatexFree failed boolean evaluation']
                    return success, df_collect_product_base_data
            else:
                df_collect_product_base_data['IsLatexFree'] = [is_latex_free]

            if 'IsRX' in row:
                success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row, 'IsRX')
                if success == False:
                    df_collect_product_base_data['FinalReport'] = ['IsRX failed boolean evaluation']
                    return success, df_collect_product_base_data
            else:
                df_collect_product_base_data['IsRX'] = [is_rx]

        return_df_line_product = self.minimum_product(df_collect_product_base_data)
        return True, return_df_line_product


    def process_long_desc(self, df_collect_product_base_data, row):
        long_desc = row['ShortDescription']
        product_name = row['ProductName']
        # processing/cleaning
        if 'LongDescription' in row:
            long_desc = row['LongDescription']
        else:
            df_collect_product_base_data['LongDescription'] = long_desc

        if 'BigCommerceProductName' in row:
            bc_product_name = row['BigCommerceProductName']
        else:
            bc_product_name = product_name

        if 'ECommerceLongDescription' in row:
            ec_long_desc = row['ECommerceLongDescription']
        else:
            ec_long_desc = long_desc

        if len(ec_long_desc) > 700:
            ec_long_desc = ec_long_desc[:700]


        df_collect_product_base_data['BigCommerceProductName'] = [bc_product_name]
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

        if 'IsFreeShipping' in row:
            success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row, 'IsFreeShipping')
            if success:
                is_free_shipping = row['IsFreeShipping']

        if 'IsColdChain' in row:
            success, df_collect_product_base_data = self.process_boolean(df_collect_product_base_data, row, 'IsColdChain')
            if success:
                is_cold_chain = row['IsColdChain']


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
            manufacturer_product_id = row['ManufacturerPartNumber']
            fy_catalog_number = row['FyCatalogNumber']
            product_name = row['ProductName']

            bc_product_name = row['BigCommerceProductName']
            ec_long_desc = row['ECommerceLongDescription']

            country_of_origin_id = row['CountryOfOriginId']

            manufacturer_id = row['ManufacturerId']
            category_id = row['CategoryId']

            shipping_instructions_id = row['ShippingInstructionsId']
            recommended_storage_id = row['RecommendedStorageId']
            expected_lead_time_id = row['ExpectedLeadTimeId']

            is_controlled = row['IsControlled']
            is_disposible = row['IsDisposable']
            is_green = row['IsGreen']
            is_latex_free = row['IsLatexFree']
            is_rx = row['IsRX']

        self.obIngester.ingest_product(self.is_last, fy_catalog_number, manufacturer_product_id, product_name,
                                                 bc_product_name, ec_long_desc, country_of_origin_id, manufacturer_id,
                                                 shipping_instructions_id, recommended_storage_id,
                                                 expected_lead_time_id, category_id, is_controlled, is_disposible,
                                                 is_green, is_latex_free, is_rx)

        return df_line_product


## end ##