# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210610
# CreateFor: Franklin Young International

import os
import shutil
import pandas
import requests

from PIL import Image
from PIL import UnidentifiedImageError

from Tools.FY_DAL import S3Object
from Tools.BasicProcess import BasicProcessObject

class FileProcessor(BasicProcessObject):
    req_fields = []
    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, proc_to_set, aws_access_key_id, aws_secret_access_key, skip_manufacturers = False):
        self.proc_to_run = proc_to_set
        self.skip_manufacturers = skip_manufacturers
        super().__init__(df_product, user, password, is_testing)
        self.name = 'File Processor'
        self.lindas_increase = 0.25
        if self.proc_to_run == 'Load Manufacturer Default Image':
            self.obS3 = S3Object(aws_access_key_id, aws_secret_access_key)


    def header_viability(self):
        if self.proc_to_run == 'Extract Attributes':
            self.req_fields = []
            self.sup_fields = ['FyProductName','VendorProductName','FyProductDescription','VendorProductDescription']

        if self.proc_to_run == 'Extract Configuration':
            self.req_fields = []
            self.sup_fields = ['FyProductName','VendorProductName','FyProductDescription','VendorProductDescription']

        if self.proc_to_run == 'Assign FyPartNumbers':
            self.req_fields = ['ManufacturerName', 'ManufacturerPartNumber','UnitOfIssue']
            self.sup_fields = []

        if self.proc_to_run == 'Unicode Correction':
            self.req_fields = []
            self.sup_fields = ['FyProductName','VendorProductName','FyProductDescription','VendorProductDescription']

        if self.proc_to_run == 'Load Manufacturer Default Image':
            self.req_fields = ['ManufacturerName', 'ImagePath']
            self.sup_fields = []


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


    def batch_preprocessing(self):
        if self.proc_to_run == 'Assign FyPartNumbers':
            # 'FyCatalogNumber', 'FyCatalogNumber', 'ManufacturerName', 'ManufacturerPartNumber','db_IsProductNumberOverride'
            self.df_override_lookup = self.obDal.get_overrides()
            self.df_override_lookup = self.df_override_lookup.drop(columns=['FyProductNumber','UnitOfIssue'])
            match_set = ['ManufacturerName', 'ManufacturerPartNumber']
            self.df_product = self.df_product.merge(self.df_override_lookup, how='left', on = match_set)

        if self.proc_to_run == 'Load Manufacturer Default Image':
            self.batch_process_manufacturer()
            self.lst_image_objects = self.obS3.get_object_list('franklin-young-image-bank')


    def process_product_line(self, df_line_product):
        self.success = True
        if self.proc_to_run == 'Extract Attributes':
            self.success, df_line_product = self.extract_attributes(df_line_product)

        elif self.proc_to_run == 'Assign FyPartNumbers':
            self.success, df_line_product = self.assign_fy_part_numbers(df_line_product)

        elif self.proc_to_run == 'Unicode Correction':
            self.success, df_line_product = self.correct_bad_unicode(df_line_product)

        elif self.proc_to_run == 'Extract Configuration':
            self.success, df_line_product = self.extract_configuration(df_line_product)

        elif self.proc_to_run == 'Load Manufacturer Default Image':
            self.success, df_line_product = self.manufacturer_default_images(df_line_product)

        else:
            self.obReporter.report_no_process()
            self.success = False


        return self.success, df_line_product

    def correct_bad_unicode(self, df_line_product):
        self.success = True
        clean_up_columns = ['FyProductName','VendorProductName','VendorProductDescription','FyProductDescription']
        df_collect_line = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            for each_column_to_clean in clean_up_columns:
                if each_column_to_clean in row:
                    product_name = row[each_column_to_clean]
                    product_name = self.obValidator.clean_description(product_name)

                    if self.obValidator.isEnglish(product_name) == False:
                        product_name = self.obValidator.remove_unicode(product_name)

                        if self.obValidator.isEnglish(product_name) == False:
                            self.obReporter.update_report('Alert','Review for unicode ' + each_column_to_clean)

                    df_collect_line[each_column_to_clean] = [product_name]

        return self.success, df_collect_line


    def manufacturer_default_images(self,df_line_product):
        success = True

        df_collect_line = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            manufacturer_name = row['ManufacturerName']

            path_manufacturer_name = manufacturer_name.replace(',', '')
            path_manufacturer_name = path_manufacturer_name.replace(' ', '_')

            image_path = row['ImagePath']
            bucket = 'franklin-young-image-bank'

            if 'http' in image_path:
                # asset path is a url to fetch
                # this is the name of the file as pulled from the url
                url_name = image_path.rpartition('/')[2]
                # this is the path which is placed, relatively to CWD
                temp_path = 'temp_asset_files\\' + url_name
                # This is the true path to the file
                whole_path = str(os.getcwd()) + '\\' + temp_path
                df_collect_line['WholeFilePath'] = [whole_path]

                if os.path.exists(whole_path):
                    object_name = whole_path.rpartition('\\')[2]
                    self.obReporter.update_report('Alert', 'This was previously scraped')
                else:
                    # Make http request for remote file data
                    asset_data = requests.get(image_path)


                    if asset_data.ok:
                        # Save file data to local copy
                        with open(temp_path, 'wb')as file:
                            file.write(asset_data.content)
                        object_name = whole_path.rpartition('\\')[2]
                        df_collect_line['AssetObjectName'] = [object_name]
                        self.obReporter.update_report('Alert', 'This asset was scraped')
                    else:
                        self.obReporter.update_report('Fail', 'This url doesn\'t work.')
                        return False, df_collect_line

            elif os.path.exists(image_path):
                object_name = image_path.rpartition('\\')[2]
                whole_path = image_path
                df_collect_line['AssetObjectName'] = [object_name]
            else:
                self.obReporter.update_report('Alert', 'Please check that the path is a url or file path')
                return False, df_collect_line


            s3_name = path_manufacturer_name + '/' + object_name

            # we check if the documents
            if 'CurrentAssetPath' in row:
                current_asset_path = row['CurrentAssetPath']
                if current_asset_path == whole_path and asset_type != 'Image' and asset_type != 'Video':
                    self.obReporter.update_report('Fail', 'This asset already exists.')
                    return False, df_collect_line
                else:
                    self.obReporter.update_report('Alert', 'This product asset was overwritten.')

            self.obS3.put_file(whole_path, s3_name, bucket)
            if s3_name not in self.lst_image_objects:
                self.lst_image_objects.append(s3_name)

            # the size can't fail
            image_width, image_height = self.get_image_size(whole_path)

            success = self.obDal.set_manufacturer_default_image(manufacturer_name, s3_name, object_name, image_width, image_height)
            if not success:
                self.obReporter.update_report('Fail','Failed at ingestion')

        return success, df_collect_line


    def get_image_size(self, image_path):
        try:
            current_image = Image.open(image_path)
            image_width, image_height = current_image.size
        except UnidentifiedImageError:
            image_width, image_height = 0, 0

        return image_width, image_height


    def extract_configuration(self, df_line_product):
        self.success = True
        df_collect_attribute_data = df_line_product.copy()
        extract_set = ['FyProductName','VendorProductName','FyProductDescription','VendorProductDescription']

        for each_col in extract_set:
            if each_col in df_line_product.columns:
                for colName, row in df_line_product.iterrows():
                    self.success, df_collect_attribute_data, product_name = self.process_configuration(df_collect_attribute_data, row, row[each_col])
                    product_name = self.obExtractor.reinject_phrase(product_name)
                    df_collect_attribute_data[each_col] = [product_name]
                df_line_product = df_collect_attribute_data.copy()

        return True, df_collect_attribute_data


    def generate_pricing(self, df_collect_product_base_data, row):
        fy_cost = round(float(row['FyCost']),2)
        df_collect_product_base_data['FyCost'] = [fy_cost]

        if 'VendorListPrice' in row and 'Discount' in row:
            vendor_list_price = round(float(row['VendorListPrice']), 2)
            fy_discount_percent = round(float(row['Discount']), 2)
            if fy_discount_percent != 0:
                # discount and cost
                fy_cost_test = vendor_list_price - round((vendor_list_price * fy_discount_percent), 2)
                check_val = abs(fy_cost_test - fy_cost)
                # we trust the cost provided over the discount given
                if check_val > 0.01:
                    fy_discount_percent = round((1 - (fy_cost / vendor_list_price)), 2)
                    df_collect_product_base_data['Discount'] = [fy_discount_percent]

        elif 'Discount' in row:
            fy_discount_percent = round(float(row['Discount']), 2)
            if fy_discount_percent != 0:
                vendor_list_price = round(fy_cost/(1-fy_discount_percent), 2)
                df_collect_product_base_data['VendorListPrice'] = [vendor_list_price]
            else:
                df_collect_product_base_data['VendorListPrice'] = [0]

        elif 'VendorListPrice' in row:
            vendor_list_price = round(float(row['VendorListPrice']), 2)
            if vendor_list_price != 0:
                fy_discount_percent = round(1 - (fy_cost / vendor_list_price), 2)
                df_collect_product_base_data['Discount'] = fy_discount_percent
            else:
                df_collect_product_base_data['Discount'] = [0]
        else:
            df_collect_product_base_data['Discount'] = [0]
            df_collect_product_base_data['VendorListPrice'] = [0]

        if 'Fixed Shipping Cost' not in row:
            estimated_freight = 0
            df_collect_product_base_data['Fixed Shipping Cost'] = estimated_freight
        else:
            estimated_freight = round(float(row['Fixed Shipping Cost']), 2)

        fy_landed_cost = round(fy_cost + estimated_freight, 2)
        df_collect_product_base_data['Landed Cost'] = [fy_landed_cost]

        if 'LandedCostMarkupPercent_FYSell' in row and 'ECommerceDiscount' not in row and 'Retail Price' not in row:
            mark_up_sell = round(float(row['LandedCostMarkupPercent_FYSell']), 2)
            fy_sell_price = round(fy_landed_cost * mark_up_sell, 2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]

            if 'LandedCostMarkupPercent_FYList' not in row:
                mark_up_list = mark_up_sell + self.lindas_increase
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [mark_up_list]
            else:
                mark_up_list = round(float(row['LandedCostMarkupPercent_FYList']), 2)
            fy_list_price = round(fy_landed_cost * mark_up_list, 2)
            df_collect_product_base_data['Retail Price'] = [fy_list_price]

            df_collect_product_base_data['ECommerceDiscount'] = [round(1-float(fy_sell_price/fy_list_price), 2)]

        elif ('ECommerceDiscount' in row or 'MfcDiscountPercent' in row) and 'Retail Price' not in row and 'LandedCostMarkupPercent_FYList' in row:
            mark_up_list = round(float(row['LandedCostMarkupPercent_FYList']), 2)
            fy_list_price = round(fy_landed_cost * mark_up_list, 2)
            df_collect_product_base_data['Retail Price'] = fy_list_price

            if 'ECommerceDiscount' not in row:
                ecommerce_discount = round(float(row['MfcDiscountPercent']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [ecommerce_discount]
                self.obReporter.update_report('Alert', 'MfcDiscountPercent was used in place of ECommerceDiscount.')
            else:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)

            fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]


        elif 'Retail Price' in row and ('ECommerceDiscount' in row or 'MfcDiscountPercent' in row):
            fy_list_price = round(float(row['Retail Price']), 2)
            mark_up_list = round(float(fy_list_price/fy_landed_cost), 2)

            if 'LandedCostMarkupPercent_FYList' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [mark_up_list]

            if 'ECommerceDiscount' not in row:
                mfc_discount = round(float(row['MfcDiscountPercent']), 2)
                df_collect_product_base_data['ECommerceDiscount'] = [mfc_discount]
                self.obReporter.update_report('Alert', 'MfcDiscountPercent was used in place of ECommerceDiscount.')
            else:
                ecommerce_discount = round(float(row['ECommerceDiscount']), 2)

            fy_sell_price = round(fy_list_price-(fy_list_price*ecommerce_discount),2)
            df_collect_product_base_data['Sell Price'] = [fy_sell_price]
            df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [round(float(fy_sell_price/fy_landed_cost), 2)]

        else:
            if 'LandedCostMarkupPercent_FYSell' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYSell'] = [0]

            if 'Sell Price' not in row:
                df_collect_product_base_data['Sell Price'] = [0]

            if 'LandedCostMarkupPercent_FYList' not in row:
                df_collect_product_base_data['LandedCostMarkupPercent_FYList'] = [0]

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]

            if 'ECommerceDiscount' not in row:
                df_collect_product_base_data['ECommerceDiscount'] = [0]

            if 'Retail Price' not in row:
                df_collect_product_base_data['Retail Price'] = [0]

            self.obReporter.update_report('Alert', 'Basic pricing was loaded.')


        return True, df_collect_product_base_data


    def assign_fy_part_numbers(self, df_line_product):
        success = True
        df_collect_attribute_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            success, df_collect_attribute_data, fy_manufacturer_prefix = self.process_manufacturer(df_collect_attribute_data, row, self.skip_manufacturers)

            if not success:
                return success, df_collect_attribute_data

            if 'UnitOfIssue' in row:
                unit_of_issue = self.normalize_units(row['UnitOfIssue'])
            else:
                return False, df_collect_attribute_data

            b_override = False
            success, return_val = self.process_boolean(row, 'db_IsProductNumberOverride')
            if success and return_val == 1:
                b_override = True
                df_collect_attribute_data['FyProductNumberOverride'] = [return_val]

            if not b_override:
                success, return_val = self.process_boolean(row, 'FyProductNumberOverride')
                if success and return_val == 1:
                    b_override = True
                    df_collect_attribute_data['FyProductNumberOverride'] = [return_val]

            manufacturer_part_number = str(row['ManufacturerPartNumber'])

            fy_catalog_number, fy_product_number = self.build_part_number(row, manufacturer_part_number, fy_manufacturer_prefix, unit_of_issue, b_override)

            if (fy_catalog_number != fy_product_number) and (fy_catalog_number not in fy_product_number):
                self.obReporter.update_report('Fail','Catalog number/Product number disagree')
                return False, df_collect_attribute_data

            df_collect_attribute_data['FyCatalogNumber'] = [fy_catalog_number]
            df_collect_attribute_data['FyProductNumber'] = [fy_product_number]

            for each_header in ['FyCatalogNumber_x','FyCatalogNumber_y','FyProductNumber_x','FyProductNumber_y']:
                if each_header in df_collect_attribute_data.columns:
                    df_collect_attribute_data = df_collect_attribute_data.drop(columns=[each_header])

        return True, df_collect_attribute_data


    def extract_attributes(self, df_line_product):
        success = True
        df_collect_attribute_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'FyProductName' in row:
                success, df_collect_attribute_data, fy_product_name = self.process_long_desc(df_collect_attribute_data, row, row['FyProductName'])
                fy_product_name = self.obExtractor.reinject_phrase(fy_product_name)
                df_collect_attribute_data['FyProductName'] = [fy_product_name]

            if 'VendorProductName' in row:
                success, df_collect_attribute_data, vendor_product_name = self.process_long_desc(df_collect_attribute_data, row, row['VendorProductName'])
                vendor_product_name = self.obExtractor.reinject_phrase(long_desc)
                df_collect_attribute_data['VendorProductName'] = [vendor_product_name]
            if 'FyProductDescription' in row:
                success, df_collect_attribute_data, fy_product_description = self.process_long_desc(df_collect_attribute_data, row, row['FyProductDescription'])
                fy_product_description = self.obExtractor.reinject_phrase(fy_product_description)
                df_collect_attribute_data['FyProductDescription'] = [fy_product_description]
            if 'VendorProductDescription' in row:
                success, df_collect_attribute_data, vendor_product_description = self.process_long_desc(df_collect_attribute_data, row, row['VendorProductDescription'])
                vendor_product_description = self.obExtractor.reinject_phrase(vendor_product_description)
                df_collect_attribute_data['VendorProductDescription'] = [vendor_product_description]


        return True, df_collect_attribute_data


    def process_long_desc(self, df_collect_product_base_data, row, container_str):
        # things to extract:
        # uoi details
        # things with names. eg. 'thickness: 3.2mm.'
        # material might be messy
        # particle sizes
        # handle mu character
        # Cas numbers
        # time, duration
        # guage
        # decibels
        # 7.8/10.0mm, 8/9/10mm etc. (this is ugly)
        # (0-.2 ppm) or any similar problem
        # amperage

        catch = ''
        if 'UnitOfIssue' not in row:
            container_str, attribute = self.obExtractor.extract_uoi(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue'] = attribute

        if 'Dimensions' not in row:
            container_str, attribute = self.obExtractor.extract_dimensions(container_str)
            if attribute != '':
                df_collect_product_base_data['Dimensions'] = attribute

        if 'TemperatureRange' not in row:
            container_str, attribute = self.obExtractor.extract_temp_range(container_str)
            if attribute != '':
                df_collect_product_base_data['TemperatureRange'] = attribute

        if 'Temperature' not in row:
            container_str, attribute = self.obExtractor.extract_temperature(container_str)
            if attribute != '':
                df_collect_product_base_data['Temperature'] = attribute

        if 'Electrical' not in row:
            container_str, attribute = self.obExtractor.extract_electrical(container_str)
            if attribute != '':
                df_collect_product_base_data['Electrical'] = attribute

        if 'VoltageRange' not in row:
            container_str, attribute = self.obExtractor.extract_voltage_range(container_str)
            if attribute != '':
                df_collect_product_base_data['VoltageRange'] = attribute

        if 'Voltage' not in row:
            container_str, attribute = self.obExtractor.extract_voltage(container_str)
            if attribute != '':
                df_collect_product_base_data['Voltage'] = attribute

        if 'Wattage' not in row:
            container_str, attribute = self.obExtractor.extract_wattage(container_str)
            if attribute != '':
                df_collect_product_base_data['Wattage'] = attribute

        if 'Frequency' not in row:
            container_str, attribute = self.obExtractor.extract_frequency(container_str)
            if attribute != '':
                df_collect_product_base_data['Frequency'] = attribute

        if 'PartsPerMillion' not in row:
            container_str, attribute = self.obExtractor.extract_ppm(container_str)
            if attribute != '':
                df_collect_product_base_data['PartsPerMillion'] = attribute

        if 'Concentration' not in row:
            container_str, attribute = self.obExtractor.extract_concentration(container_str)
            if attribute != '':
                df_collect_product_base_data['Concentration'] = attribute

        if 'Pressure' not in row:
            container_str, attribute = self.obExtractor.extract_pressure(container_str)
            if attribute != '':
                df_collect_product_base_data['Pressure'] = attribute

        if 'Rate' not in row:
            container_str, attribute = self.obExtractor.extract_rate(container_str)
            if attribute != '':
                df_collect_product_base_data['Rate'] = attribute

        if 'Color' not in row:
            container_str, attribute = self.obExtractor.extract_color(container_str)
            if attribute != '':
                df_collect_product_base_data['Color'] = attribute

        # you are here
        if 'WeightRange' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_weight_range)
            if attribute != '':
                df_collect_product_base_data['WeightRange'] = attribute

        if 'Weight' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_weight)
            if attribute != '':
                df_collect_product_base_data['Weight'] = attribute

        if 'VolumeRange' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_volume_range)
            if attribute != '':
                df_collect_product_base_data['VolumeRange'] = attribute

        if 'Volume' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_volume)
            if attribute != '':
                df_collect_product_base_data['Volume'] = attribute

        if 'LengthRange' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_length_range)
            if attribute != '':
                df_collect_product_base_data['LengthRange'] = attribute

        if 'Length' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_length)
            if attribute != '':
                df_collect_product_base_data['Length'] = attribute

        if 'Thickness' not in row:
            container_str, attribute = self.obExtractor.extract_attributes(container_str, self.obExtractor.pat_for_thickness)
            if attribute != '':
                df_collect_product_base_data['Thickness'] = attribute

            # add else standardize
        # rinse and repeat


        return True, df_collect_product_base_data, container_str


    def process_configuration(self, df_collect_product_base_data, row, container_str):

        if 'UnitOfIssue_8' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_8(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_8'] = attribute
        else:
            attribute_left = str(row['UnitOfIssue_8'])
            container_str, attribute_right = self.obExtractor.extract_uoi_8(container_str)
            if attribute_right != '':
                df_collect_product_base_data['UnitOfIssue_8'] = attribute_left + '; ' + attribute_right

        if 'UnitOfIssue_5' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_5(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_5'] = attribute

        if 'UnitOfIssue_7' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_7(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_7'] = attribute

        if 'UnitOfIssue_4' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_4(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_4'] = attribute

        if 'UnitOfIssue_3' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_3(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_3'] = attribute

        if 'UnitOfIssue_2' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_2(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_2'] = attribute

        if 'UnitOfIssue_1' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_1(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_1'] = attribute

        if 'UnitOfIssue_6' not in row:
            container_str, attribute = self.obExtractor.extract_uoi_6(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue_6'] = attribute

        if 'UnitOfIssue' not in row:
            container_str, attribute = self.obExtractor.extract_uoi(container_str)
            if attribute != '':
                df_collect_product_base_data['UnitOfIssue'] = attribute

        return True, df_collect_product_base_data, container_str



## end ##