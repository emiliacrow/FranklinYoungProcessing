# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210610
# CreateFor: Franklin Young International

from Tools.BasicProcess import BasicProcessObject

class FileProcessor(BasicProcessObject):
    req_fields = []
    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, proc_to_set):
        self.proc_to_run = proc_to_set
        super().__init__(df_product, user, password, is_testing)
        self.name = 'File Processor'
        self.lindas_increase = 0.25


    def header_viability(self):
        if self.proc_to_run == 'Extract Attributes':
            self.req_fields = []
            self.sup_fields = ['LongDescription', 'ShortDescription','ProductDescription']

        if self.proc_to_run == 'Assign FyPartNumbers':
            self.req_fields = ['ManufacturerName', 'ManufacturerPartNumber']
            self.sup_fields = []

        if self.proc_to_run == 'Unicode Correction':
            self.req_fields = ['ProductName']
            self.sup_fields = []

        if self.proc_to_run == 'Generate Upload File':
            self.set_new_order = True
            self.out_column_headers = ['Pass','Alert','Fail','FyCatalogNumber','FyProductNumber','FyPartNumber','Item Type', 'ProductName', 'Product Type',
                                       'Product Code/SKU', 'Brand Name', 'Option Set Align', 'Product Description',
                                       'VendorListPrice', 'Discount', 'FyCost', 'Fixed Shipping Cost',
                                       'FyLandedCost', 'LandedCostMarkupPercent_FYSell','Sell Price',
                                       'LandedCostMarkupPercent_FYList', 'Retail Price', 'ECommerceDiscount',
                                       'Free Shipping', 'Product Weight', 'Product Width', 'Product Height',
                                       'Product Depth', 'Allow Purchases?', 'Product Visible?', 'Track Inventory',
                                       'Current Stock Level', 'Low Stock Level', 'Category', 'Product Image File - 1',
                                       'Product Image Description - 1', 'Product Image Sort - 1', 'Product Condition',
                                       'Show Product Condition?', 'Sort Order', 'Product Tax Class',
                                       'Stop Processing Rules', 'ProductUrl', 'GPS Manufacturer Part Number',
                                       'GPS Enabled', 'Tax Provider Tax Code', 'Product Custom Fields',
                                       'ShortDescription','LongDescription','Hazmat','Add to Website/GSA',
                                       'Conv Factor/QTY UOM','CountryOfOrigin','ManufacturerName',
                                       'ManufacturerPartNumber','SupplierName','Temp Control',
                                       'VendorPartNumber','UnitOfMeasure','UNSPSC','VendorName']

            self.req_fields = ['FyPartNumber','ShortDescription', 'FyCost',
                               'ManufacturerName','ManufacturerPartNumber', 'Category']
            self.sup_fields = []

        # inital file viability check
        product_headers = set(self.lst_product_headers)
        required_headers = set(self.req_fields)
        overlap = list(required_headers.intersection(product_headers))
        if len(overlap) >= 1:
            self.is_viable = True


    def process_product_line(self, df_line_product):
        self.success = True
        if self.proc_to_run == 'Extract Attributes':
            self.success, df_line_product = self.extract_attributes(df_line_product)

        elif self.proc_to_run == 'Assign FyPartNumbers':
            self.success, df_line_product = self.assign_fy_part_numbers(df_line_product)

        elif self.proc_to_run == 'Unicode Correction':
            self.success, df_line_product = self.correct_bad_unicode(df_line_product)

        elif self.proc_to_run == 'Generate Upload File':
            self.success, df_line_product = self.generate_BC_upload(df_line_product)

        else:
            self.obReporter.report_no_process()
            self.success = False


        return self.success, df_line_product

    def correct_bad_unicode(self, df_line_product):
        self.success = True
        clean_up_columns = ['ProductName','LongDescription','ShortDescription','ECommerceLongDescription','ProductDescription']
        df_collect_line = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            for each_column_to_clean in clean_up_columns:
                if each_column_to_clean in row:
                    product_name = row[each_column_to_clean]
                    if self.obValidator.isEnglish(product_name) == False:
                        product_name = self.obValidator.remove_unicode(product_name)
                        df_collect_line[each_column_to_clean] = [product_name]
                        temp_prod_name = product_name.replace('º','')
                        if self.obValidator.isEnglish(product_name) == False:
                            self.obReporter.update_report('Alert','Review for unicode ' + each_column_to_clean)

        return self.success, df_collect_line

    def generate_BC_upload(self, df_line_product):
        self.success = True

        df_collect_line = df_line_product.copy()

        for colName, row in df_line_product.iterrows():
            # they all get this
            df_collect_line['Product Width'] = ['0']
            df_collect_line['Product Height'] = ['0']
            df_collect_line['Product Depth'] = ['0']
            df_collect_line['Product Weight'] = ['1']

            df_collect_line['Show Product Condition?'] = ['N']
            df_collect_line['Product Condition'] = ['New']
            df_collect_line['Track Inventory'] = ['none']
            df_collect_line['Current Stock Level'] = ['0']
            df_collect_line['Low Stock Level'] = ['0']
            df_collect_line['Sort Order'] = ['0']

            df_collect_line['Stop Processing Rules'] = ['N']
            df_collect_line['Free Shipping'] = ['N']
            df_collect_line['Fixed Shipping Cost'] = ['0']
            df_collect_line['Sale Price'] = ['0']

            df_collect_line['GPS Enabled'] = ['N']
            df_collect_line['Option Set Align'] = ['Right']
            df_collect_line['Allow Purchases?'] = ['Y']
            df_collect_line['Product Visible?'] = ['Y']
            df_collect_line['Product Type'] = ['P']
            df_collect_line['Tax Provider Tax Code'] = ['NonTaxable']
            df_collect_line['Product Tax Class'] = ['Default Tax Class']
            df_collect_line['Item Type'] = ['Product']

            # if GSA there is something else to generate "GSA - Sch 66"
            # this generates the descriptions
            short_desc = row['ShortDescription']
            if 'LongDescription' in row:
                long_desc = row['LongDescription']
            else:
                long_desc = short_desc
                df_collect_line['LongDescription'] = [short_desc]

            df_collect_line['Product Description'] = [long_desc]

            # this deals with product numbers
            manufacturer_name = row['ManufacturerName']
            df_collect_line['Brand Name'] = [manufacturer_name]
            manufacturer_part_number = row['ManufacturerPartNumber']
            df_collect_line['GPS Manufacturer Part Number'] = [manufacturer_part_number]

            fy_part_number = row['FyPartNumber']
            df_collect_line['Product Code/SKU'] = [fy_part_number]

            if 'ProductName' not in row:
                if len(short_desc) > 40:
                    product_name  = short_desc[:40]
                else:
                    product_name  = short_desc
                df_collect_line['ProductName'] = [product_name]

            else:
                product_name = row['ProductName']
                if len(product_name) > 40:
                    product_name  = product_name[:40]

            # this section should generate pricing correctly
            pricing_success, df_collect_line = self.generate_pricing(df_collect_line, row)
            if pricing_success == False:
                return pricing_success, df_collect_line


            # this deals with the image
            if 'ImageName' not in row:
                df_collect_line['Product Image File - 1'] = ['']
                df_collect_line['Product Image Sort - 1'] = ['']
                self.obReporter.update_report('Alert','Image was missing')
            else:
                image = row['ImageName']
                df_collect_line['Product Image File - 1'] = [image]
                df_collect_line['Product Image Sort - 1'] = ['0']

            df_collect_line['Product Image Description - 1'] = [product_name]

            # this deals with the custom fields
            uoi = row['UnitOfIssue']
            if 'Conv Factor/QTY UOM' in row:
                quantity = row['Conv Factor/QTY UOM']
            else:
                quantity = '1'
                df_collect_line['Conv Factor/QTY UOM'] = [quantity]
                self.obReporter.update_report('Alert','Conv Factor was generated.')

            custom_fields = '"ShortDescription=' + short_desc +'"'
            custom_fields = custom_fields + ';unit_of_issue=' + uoi
            custom_fields = custom_fields + ';unit_of_issue_qty=' + str(quantity) + ';uom_std=EA;green_product=1;'
            custom_fields = custom_fields + '"primary_vendor_product_name=' + product_name.partition(' {')[0] +'"'

            df_collect_line['Product Custom Fields'] = [custom_fields]

        return self.success, df_collect_line

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
        self.success = True
        df_collect_attribute_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            success, df_collect_attribute_data = self.process_manufacturer(df_collect_attribute_data, row)

        return True, df_collect_attribute_data


    def extract_attributes(self, df_line_product):
        self.success = True
        df_collect_attribute_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            if 'LongDescription' in row:
                self.success, df_collect_attribute_data, long_desc = self.process_long_desc(df_collect_attribute_data, row, row['LongDescription'])
                long_desc = self.obExtractor.reinject_phrase(long_desc)
                df_collect_attribute_data['LongDescription'] = [long_desc]
            if 'ShortDescription' in row:
                self.success, df_collect_attribute_data, long_desc = self.process_long_desc(df_collect_attribute_data, row, row['ShortDescription'])
                long_desc = self.obExtractor.reinject_phrase(long_desc)
                df_collect_attribute_data['ShortDescription'] = [long_desc]
            if 'ProductDescription' in row:
                self.success, df_collect_attribute_data, long_desc = self.process_long_desc(df_collect_attribute_data, row, row['ProductDescription'])
                long_desc = self.obExtractor.reinject_phrase(long_desc)
                df_collect_attribute_data['ProductDescription'] = [long_desc]


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



