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
    def __init__(self,df_product, is_testing, proc_to_set):
        self.proc_to_run = proc_to_set
        super().__init__(df_product, is_testing)
        self.name = 'File Processor'


    def header_viability(self):
        if self.proc_to_run == 'Extract Attributes':
            self.req_fields = ['LongDescription', 'ShortDescription', 'ECommerceLongDescription']

        if self.proc_to_run == 'Assign FyPartNumbers':
            self.req_fields = ['ManufacturerName', 'ManufacturerPartNumber','UnitOfIssue']

        if self.proc_to_run == 'Unicode Correction':
            self.req_fields = ['ProductName']

        if self.proc_to_run == 'Generate BC Upload File':
            self.set_new_order = True
            self.out_column_headers = ['FinalReport','Report','Item Type', 'Product Name', 'Product Type',
                                       'Product Code/SKU', 'Brand Name', 'Option Set Align', 'Product Description',
                                       'Vendor List Price', 'Discount', 'Fy Cost', 'Fixed Shipping Cost',
                                       'FyLandedCost', 'LandedCostMarkupPercent_FYSell','Sell Price',
                                       'LandedCostMarkupPercent_FYList', 'Retail Price',
                                       'Free Shipping', 'Product Weight', 'Product Width', 'Product Height',
                                       'Product Depth', 'Allow Purchases?', 'Product Visible?', 'Track Inventory',
                                       'Current Stock Level', 'Low Stock Level', 'Category', 'Product Image File - 1',
                                       'Product Image Description - 1', 'Product Image Sort - 1', 'Product Condition',
                                       'Show Product Condition?', 'Sort Order', 'Product Tax Class',
                                       'Stop Processing Rules', 'Product URL', 'GPS Manufacturer Part Number',
                                       'GPS Enabled', 'Tax Provider Tax Code', 'Product Custom Fields',
                                       'ShortDescription','LongDescription','Hazmat','Add to Website/GSA',
                                       'Conv Factor/QTY UOM','CountryOfOrigin','ManufacturerName',
                                       'ManufacturerPartNumber','SupplierName','Temp Control',
                                       'VendorPartNumber','UnitOfMeasure','UNSPSC','VendorName','FyCatalogNumber','FyProductNumber','FyPartNumber']

            self.req_fields = ['FyPartNumber','UnitOfMeasure','ShortDescription','LongDescription',
                               'Vendor List Price','Discount','Fy Cost','LandedCostMarkupPercent_FYSell',
                               'Conv Factor/QTY UOM','Product URL','ManufacturerName','ManufacturerPartNumber',
                               'Image','Category']

        # inital file viability check
        product_headers = set(self.lst_product_headers)
        required_headers = set(self.req_fields)
        overlap = list(required_headers.intersection(product_headers))
        if len(overlap) >= 1:
            self.is_viable = True


    def line_viability(self,df_product_line):
        # line viability checks
        line_headers = set(list(df_product_line.columns))
        required_headers = set(self.req_fields)
        overlap = list(required_headers.intersection(line_headers))
        if len(overlap) >= 1:
            return True


    def process_product_line(self, df_line_product):
        self.success = True
        if self.proc_to_run == 'Extract Attributes':
            self.success, df_line_product = self.extract_attributes(df_line_product)

        elif self.proc_to_run == 'Assign FyPartNumbers':
            self.success, df_line_product = self.assign_fy_part_numbers(df_line_product)

        elif self.proc_to_run == 'Unicode Correction':
            self.success, df_line_product = self.correct_bad_unicode(df_line_product)

        elif self.proc_to_run == 'Generate BC Upload File':
            self.success, df_line_product = self.generate_BC_upload(df_line_product)

        else:
            df_line_product['Report'] = ['No process identified']
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
                        if self.obValidator.isEnglish(product_name) == False:
                            df_collect_line['Report'] = ['Review for unicode ' + each_column_to_clean]

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

            if len(short_desc) > 40:
                product_name  = short_desc[:40] + ' {' + fy_part_number + '}'
            else:
                product_name  = short_desc + ' {' + fy_part_number + '}'

            df_collect_line['Product Name'] = [product_name]

            vendor_list_price = float(row['Vendor List Price'])
            fy_discount_percent = float(row['Discount'])
            fy_cost = float(row['Fy Cost'])

            # discount and cost
            fy_cost_test = vendor_list_price - round((vendor_list_price * fy_discount_percent), 2)
            check_val = abs(fy_cost_test - fy_cost)
            # we trust the cost provided over the discount given
            if check_val > 0.01:
                fy_discount_percent = (1 - (fy_cost / vendor_list_price)) * 100
                df_collect_line['Discount'] = [fy_discount_percent]

            if 'Fixed Shipping Cost' not in row:
                estimated_freight = '0'
                df_collect_line['Fixed Shipping Cost'] = [estimated_freight]
            else:
                estimated_freight = row['Fixed Shipping Cost']

            landed_cost = round(fy_cost + float(estimated_freight), 2)
            df_collect_line['FyLandedCost'] = [landed_cost]

            sell_price = round(landed_cost * float(row['LandedCostMarkupPercent_FYSell']), 2)
            df_collect_line['Sell Price'] = [sell_price]

            if 'LandedCostMarkupPercent_FYList' not in row:
                fy_list_mu = float(row['LandedCostMarkupPercent_FYSell']) + 0.25
                df_collect_line['LandedCostMarkupPercent_FYList'] = [fy_list_mu]
            else:
                fy_list_mu = float(row['LandedCostMarkupPercent_FYList'])

            retail_price =  round(landed_cost * fy_list_mu, 2)
            df_collect_line['Retail Price'] = [retail_price]

            # this will be needed later
            # take your price round to two, multiply by MU == BC sell price

            # this deals with the image
            if 'Image' not in row:
                df_collect_line['Product Image File - 1'] = ['']
                df_collect_line['Product Image Sort - 1'] = ['']
                df_collect_line['Report'] = ['Image was missing']
            else:
                image = row['Image']
                df_collect_line['Product Image File - 1'] = [image]
                df_collect_line['Product Image Sort - 1'] = ['0']

            df_collect_line['Product Image Description - 1'] = [product_name]

            # this deals with the custom fields
            uom = row['UnitOfMeasure']
            if 'Conv Factor/QTY UOM' in row:
                quantity = row['Conv Factor/QTY UOM']
            else:
                quantity = '1'
                df_collect_line['Conv Factor/QTY UOM'] = [quantity]
                df_collect_line['Report'] = ['Conv Factor was generated.']

            custom_fields = '"ShortDescription=' + short_desc +'"'
            custom_fields = custom_fields + ';unit_of_issue=' + uom
            custom_fields = custom_fields + ';unit_of_issue_qty=' + str(quantity) + ';uom_std=EA;green_product=1;'
            custom_fields = custom_fields + '"primary_vendor_product_name=' + product_name.partition(' {')[0] +'"'

            df_collect_line['Product Custom Fields'] = [custom_fields]


        return self.success, df_collect_line

    def assign_fy_part_numbers(self, df_line_product):
        self.success = True
        df_collect_attribute_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():

            if ('ManufacturerId' not in row):
                success, df_collect_attribute_data = self.process_manufacturer(df_collect_attribute_data, row)
                if success == False:
                    df_collect_attribute_data['FinalReport'] = ['Failed in process manufacturer']
                    return success, df_collect_attribute_data

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
            if 'ECommerceLongDescription' in row:
                self.success, df_collect_attribute_data, long_desc = self.process_long_desc(df_collect_attribute_data, row, row['ECommerceLongDescription'])
                long_desc = self.obExtractor.reinject_phrase(long_desc)
                df_collect_attribute_data['ECommerceLongDescription'] = [long_desc]


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



