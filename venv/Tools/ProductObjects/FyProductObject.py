# CreatedBy: Emilia Crow
# CreateDate: 20220815
# Updated: 20220815
# CreateFor: Franklin Young International

import pandas

from Tools.ProgressBar import YesNoDialog
from Tools.BasicProcess import BasicProcessObject


class FyProductIngest(BasicProcessObject):
    req_fields = ['FyProductNumber']
    sup_fields = ['FyProductName', 'FyProductDescription', 'FyCountryOfOrigin', 'FyUnitOfIssue',
                  'FyUnitOfIssueQuantity','FyLeadTime', 'FyIsHazardous', 'PrimaryVendorName', 'SecondaryVendorName']
    att_fields = []
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'FyProduct Ingest'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.df_uois_lookup = self.obDal.get_unit_of_issue_symbol_lookup()
        if 'FyCountryOfOrigin' in self.df_product.columns:
            self.batch_process_country()

        self.batch_process_primary_vendor()

        self.df_fy_description_lookup = self.obDal.get_fy_product_descriptions()
        self.df_product = self.df_product.merge(self.df_fy_description_lookup,how='left',on=['FyProductNumber'])


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'BaseProductPriceId','BaseProductPriceId_y','BaseProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter','ProductDescriptionId','db_FyProductName','db_FyProductDescription'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def batch_process_country(self):
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

        df_attribute['FyCountryOfOriginId'] = lst_ids
        self.df_product = self.df_product.merge(df_attribute,
                                                          how='left', on=['FyCountryOfOrigin'])


    def batch_process_primary_vendor(self):
        if 'PrimaryVendorName' in self.df_product.columns:
            df_attribute = self.df_product[['PrimaryVendorName']]
            df_attribute = df_attribute.drop_duplicates(subset=['PrimaryVendorName'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                vendor_name = row['PrimaryVendorName'].upper()
                if vendor_name == '':
                    new_vendor_id = -1
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
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

            df_attribute['PrimaryVendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['PrimaryVendorName'])

        if 'SecondaryVendorName' in self.df_product.columns:
            df_attribute = self.df_product[['SecondaryVendorName']]
            df_attribute = df_attribute.drop_duplicates(subset=['SecondaryVendorName'])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                vendor_name = row['SecondaryVendorName'].upper()

                if vendor_name == '':
                    new_vendor_id = -1
                elif vendor_name in self.df_vendor_translator['VendorCode'].values:
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

            df_attribute['SecondaryVendorId'] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                     how='left', on=['SecondaryVendorName'])



    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            df_collect_product_base_data = self.identify_units(df_collect_product_base_data, row)

        df_line_product = df_collect_product_base_data.copy()

        for colName, row in df_line_product.iterrows():
            # check if it is update or not
            if 'ProductDescriptionId' in df_line_product.columns:
                success, df_collect_product_base_data  = self.update_fy_description(df_collect_product_base_data, row)
            else:
                success, df_collect_product_base_data  = self.process_fy_description(df_collect_product_base_data, row)

        return success, df_collect_product_base_data

    def identify_units(self, df_collect_product_base_data, row):
        # set quantities
        fy_unit_of_issue = -1
        if 'FyUnitOfIssue' in row:
            fy_unit_of_issue = self.normalize_units(row['FyUnitOfIssue'])
            df_collect_product_base_data['FyUnitOfIssue'] = [fy_unit_of_issue]


        if fy_unit_of_issue == -1:
            fy_unit_of_issue_symbol_id = -1
        else:
            try:
                fy_unit_of_issue_symbol_id = self.df_uois_lookup.loc[
                    (self.df_uois_lookup['UnitOfIssueSymbol'] == fy_unit_of_issue), 'UnitOfIssueSymbolId'].values[0]
            except IndexError:
                fy_unit_of_issue_symbol_id = self.obIngester.ingest_uoi_symbol(fy_unit_of_issue)

        df_collect_product_base_data['FyUnitOfIssueSymbolId'] = [fy_unit_of_issue_symbol_id]

        return df_collect_product_base_data


    def process_fy_description(self, df_collect_product_base_data, row):
        fy_product_number = row['FyProductNumber']
        if 'FyProductName' not in row:
            fy_product_name = ''
        else:
            fy_product_name = row['FyProductName']
            fy_product_name = self.obValidator.clean_description(fy_product_name)
            df_collect_product_base_data['FyProductName'] = fy_product_name

        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']
            fy_product_description = self.obValidator.clean_description(fy_product_description)
            df_collect_product_base_data['FyProductDescription'] = fy_product_description

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
            if fy_coo_id == 259:
                fy_coo_id = -1
        else:
            fy_coo_id = -1

        if 'FyUnitOfIssueSymbolId' in row:
            fy_uoi_id = int(row['FyUnitOfIssueSymbolId'])
        else:
            fy_uoi_id = -1

        if 'FyUnitOfIssueQuantity' in row:
            fy_uoi_qty = int(row['FyUnitOfIssueQuantity'])
        else:
            fy_uoi_qty = -1

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80:
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800:
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        if 'FyIsHazardous' in row:
            success, fy_is_hazardous = self.process_boolean(row, 'FyIsHazardous')
            if success:
                df_collect_product_base_data['FyIsHazardous'] = [fy_is_hazardous]
            else:
                fy_is_hazardous = -1
        else:
            fy_is_hazardous = -1

        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        report = ''
        if (fy_product_name != '' and fy_product_description != '' and fy_coo_id != -1 and fy_uoi_id != -1 and fy_uoi_qty != -1 and fy_lead_time != -1 and primary_vendor_id != -1):
            # this needs to change into an ingestor
            self.obIngester.insert_fy_product_description(fy_product_number, fy_product_name, fy_product_description,
                                                          fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time,
                                                          fy_is_hazardous, primary_vendor_id, secondary_vendor_id)
            return True, df_collect_product_base_data

        if fy_product_name == '':
            report = 'Missing FyProductName'

        if fy_product_description == '':
            if report != '':
                report = report + ', FyProductDescription'
            else:
                report = 'Missing FyProductDescription'

        if fy_coo_id == -1:
            if report != '':
                report = report + ', FyCountryOfOrigin'
            else:
                report = 'Missing FyCountryOfOrigin'

        if fy_uoi_id == -1:
            if report != '':
                report = report + ', FyUnitOfIssue'
            else:
                report = 'Missing FyUnitOfIssue'

        if fy_uoi_qty == -1:
            if report != '':
                report = report + ', FyUnitOfIssueQuantity'
            else:
                report = 'Missing FyUnitOfIssueQuantity'

        if fy_lead_time == -1:
            if report != '':
                report = report + ', FyLeadTime'
            else:
                report = 'Missing FyLeadTime'

        if primary_vendor_id == -1:
            if report != '':
                report = report + ', PrimaryVendorName'
            else:
                report = 'Missing PrimaryVendorName'

        self.obReporter.update_report('Fail', report)
        return False, df_collect_product_base_data


    def update_fy_description(self, df_collect_product_base_data, row):
        fy_product_desc_id = row['ProductDescriptionId']
        if 'FyProductName' not in row:
            fy_product_name = ''
        else:
            fy_product_name = row['FyProductName']
            fy_product_name = self.obValidator.clean_description(fy_product_name)
            df_collect_product_base_data['FyProductName'] = fy_product_name


        if 'FyProductDescription' not in row:
            fy_product_description = ''
        else:
            fy_product_description = row['FyProductDescription']
            fy_product_description = self.obValidator.clean_description(fy_product_description)
            df_collect_product_base_data['FyProductDescription'] = fy_product_description

        if 'FyCountryOfOriginId' in row:
            fy_coo_id = int(row['FyCountryOfOriginId'])
            if fy_coo_id == 259:
                fy_coo_id = -1
        else:
            fy_coo_id = -1

        if 'FyUnitOfIssueSymbolId' in row:
            fy_uoi_id = int(row['FyUnitOfIssueSymbolId'])
        else:
            fy_uoi_id = -1

        if 'FyUnitOfIssueQuantity' in row:
            fy_uoi_qty = int(row['FyUnitOfIssueQuantity'])
        else:
            fy_uoi_qty = -1

        if 'FyLeadTime' in row:
            fy_lead_time = int(row['FyLeadTime'])
        else:
            fy_lead_time = -1

        if len(fy_product_name) > 80 and fy_product_name != '':
            self.obReporter.update_report('Alert','FyProductName might be too long for some contracts.')

        if len(fy_product_description) > 800 and fy_product_description != '':
            self.obReporter.update_report('Alert','FyProductDescription might be too long for some contracts.')

        if 'FyIsHazardous' in row:
            success, fy_is_hazardous = self.process_boolean(row, 'FyIsHazardous')
            if success:
                df_collect_product_base_data['FyIsHazardous'] = [fy_is_hazardous]
            else:
                fy_is_hazardous = -1
        else:
            fy_is_hazardous = -1

        if 'PrimaryVendorId' in row:
            primary_vendor_id = int(row['PrimaryVendorId'])
        else:
            primary_vendor_id = -1

        if 'SecondaryVendorId' in row:
            secondary_vendor_id = int(row['SecondaryVendorId'])
        else:
            secondary_vendor_id = -1

        if (fy_product_name != '' or fy_product_description != '' or fy_coo_id != -1 or fy_uoi_id != -1 or fy_uoi_qty != -1 or fy_lead_time != -1 or fy_is_hazardous != -1 or primary_vendor_id != -1 or secondary_vendor_id != -1):
            self.obIngester.update_fy_product_description(fy_product_desc_id, fy_product_name, fy_product_description, fy_coo_id, fy_uoi_id, fy_uoi_qty, fy_lead_time, fy_is_hazardous, primary_vendor_id, secondary_vendor_id)

        return True, df_collect_product_base_data


    def trigger_ingest_cleanup(self):
        self.obIngester.insert_fy_product_description_cleanup()
        self.obIngester.update_fy_product_description_cleanup()


## end ##