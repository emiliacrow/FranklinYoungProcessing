# CreatedBy: Emilia Crow
# CreateDate: 20210527
# Updated: 20220210
# CreateFor: Franklin Young International


import pandas
from Tools.BasicProcess import BasicProcessObject


class FillProduct(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber']
    sup_fields = []
    att_fields = ['Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Product Fill'


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()

        self.df_image_names = self.obDal.get_image_names()

        # this is here to consume values that may be more alike than different
        # for example, recommended storage might have a lot of 'keep this darn thing cold'
        if 'Sterility' in self.df_product.columns:
            self.batch_process_attribute('Sterility')
        if 'SurfaceTreatment' in self.df_product.columns:
            self.batch_process_attribute('SurfaceTreatment')
        if 'Precision' in self.df_product.columns:
            self.batch_process_attribute('Precision')


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def batch_process_attribute(self,attribute):
        str_attribute_id = attribute +'Id'
        if str_attribute_id not in self.df_product.columns:
            df_attribute = self.df_product[[attribute,str_attribute_id]]
            df_attribute = df_attribute.drop_duplicates(subset=[attribute,str_attribute_id])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                attribute_phrase = row[attribute]
                attribute_phrase = attribute_phrase[:128]

                attribute_id = self.obIngester.ingest_attribute(attribute_phrase, attribute)
                lst_ids.append(attribute_id)

            df_attribute[str_attribute_id] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                              how='left', on=[attribute])


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'ConfigurationChanges', 'New', 'PartNumberOverride', 'Partial', 'Possible Duplicate', 'Ready', 'Update-product', 'Update-vendor', 'VendorPartNumberChange']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product')
            return False

        elif row['Filter'] in ['ConfigurationChanges','Partial','PartNumberOverride', 'VendorPartNumberChange', 'Base Pricing','Update-product','Update-vendor']:
            self.obReporter.update_report('Alert', 'Passed filtering as new configuration')
            return True

        elif row['Filter'] == 'Ready':
            self.obReporter.update_report('Alert', 'Passed filtering as ready')
            return False

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Alert', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
            return False



    def process_product_line(self, df_line_product):
        df_collect_product_base_data = df_line_product.copy()
        df_collect_product_base_data = self.process_attribute_data(df_collect_product_base_data)

        for colName, row in df_line_product.iterrows():
            # this filters out the fails
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            product_id = row['ProductId']
            if product_id == -1:
                self.obReporter.update_report('Fail','Couldn\'t identify product id')
                return False, df_collect_product_base_data
            else:
                df_collect_product_base_data['ProductId'] = [product_id]

            for each_bool in ['IsControlled', 'IsDisposable', 'IsGreen', 'IsLatexFree', 'IsRX', 'IsHazardous',
                              'IsFreeShipping', 'IsColdChain']:
                success, return_val = self.process_boolean(row, each_bool)
                if success:
                    df_collect_product_base_data[each_bool] = [return_val]
                else:
                    df_collect_product_base_data[each_bool] = [0]

            # remove
            is_controlled = row['IsControlled']
            is_disposible = row['IsDisposable']
            is_green = row['IsGreen']
            is_latex_free = row['IsLatexFree']
            is_rx = row['IsRX']
            is_hazardous = row['IsHazardous']

            self.process_image(row, product_id)

            nato_stock_number = self.process_nsn(row)

            model_number = self.process_model_number(row)
            required_sample_size = self.process_required_sample_size(row)
            number_of_channels = self.process_number_of_channels(row)
            GTIN = self.process_gtin(row)

            sterility_id = self.process_sterility(row)
            surface_treatment_id = self.process_surface_treatement(row)
            precision_id = self.process_precision(row)
            product_seo_id = self.process_product_seo(row)

            component_set_id = self.process_component_set(row)

            FSC_code_id = self.process_fsc(row)
            hazardous_code_id = self.process_hazardous_code(row)
            UNSPSC_id = self.process_unspsc(row)
            NAICS_code_id = self.process_naics(row)

            national_drug_code_id = self.process_national_drug_code(row)
            product_warranty_id = self.process_product_warranty(row)
            species_id = self.process_species(row)

        self.obIngester.fill_product(product_id, nato_stock_number, model_number, required_sample_size,
                                     number_of_channels, GTIN, sterility_id, surface_treatment_id, precision_id,
                                     product_seo_id, component_set_id, FSC_code_id, hazardous_code_id, UNSPSC_id,
                                     NAICS_code_id, national_drug_code_id, product_warranty_id, species_id,
                                     is_controlled, is_disposible, is_green, is_latex_free, is_rx, is_hazardous)

        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.fill_product_cleanup()

    def process_nsn(self, row):
        nato_stock_number = ''
        if 'NatoStockNumber' in row:
            nato_stock_number = row['NatoStockNumber']

        return nato_stock_number

    def process_model_number(self, row):
        model_number = ''
        if 'ModelNumber' in row:
            model_number = row['ModelNumber']

        return model_number

    def process_required_sample_size(self, row):
        required_sample_size = ''
        if 'RequiredSampleSize' in row:
            required_sample_size = row['RequiredSampleSize']

        return required_sample_size

    def process_number_of_channels(self, row):
        number_of_channels = ''
        if 'NumberOfChannels' in row:
            number_of_channels = row['NumberOfChannels']

        return number_of_channels

    def process_gtin(self, row):
        GTIN = ''
        if 'GTIN' in row:
            GTIN = row['GTIN']

        return GTIN

    def process_sterility(self, row):
        sterility_id = -1
        if 'SterilityId' in row:
            sterility_id = row['SterilityId']

        return sterility_id

    def process_surface_treatement(self, row):
        surface_treatment_id = -1
        if 'SurfaceTreatmentId' in row:
            surface_treatment_id = row['SurfaceTreatmentId']

        return surface_treatment_id

    def process_precision(self, row):
        precision_id = -1
        if 'PrecisionId' in row:
            precision_id = row['PrecisionId']

        return precision_id

    def process_product_seo(self, row):
        product_seo_id = -1
        if 'ProductSEOId' in row:
            product_seo_id = row['ProductSEOId']


        return product_seo_id

    def process_component_set(self, row):
        component_set_id = -1
        if 'ComponentSetId' in row:
            component_set_id = row['ComponentSetId']

        return component_set_id

    def process_fsc(self,row):
        FSC_code_id = -1
        if 'FSCCodeId' in row:
            FSC_code_id = row['FSCCodeId']
        elif 'FSCCode' in row:
            if 'FSCCodeName' in row:
                FSC_code_id = self.obIngester.get_fsc_id(row['FSCCode'], row['FSCCodeName'])
            else:
                FSC_code_id = self.obIngester.get_fsc_id(row['FSCCode'])

        return FSC_code_id

    def process_hazardous_code(self, row):
        hazardous_code_id = -1
        if 'HazardousCodeId' in row:
            hazardous_code_id = row['HazardousCodeId']

        return hazardous_code_id

    def process_unspsc(self, row):
        UNSPSC_id = -1
        if 'UNSPSCId' in row:
            UNSPSC_id = row['UNSPSCId']
        elif 'UNSPSCCode' in row:
            if 'UNSPSCDesc' in row:
                UNSPSC_id = self.obIngester.get_unspsc_id(row['UNSPSCCode'],row['UNSPSCDesc'])
            else:
                UNSPSC_id = self.obIngester.get_unspsc_id(row['UNSPSCCode'])

        return UNSPSC_id

    def process_naics(self, row):
        NAICS_code_id = -1
        if 'NAICSCodeId' in row:
            NAICS_code_id = row['NAICSCodeId']
        elif 'NAICSCode' in row:
            if 'NAICSCodeDesc' in row:
                NAICS_code_id = self.obIngester.ingest_naics_code(row['NAICSCode'],row['NAICSCodeDesc'])
            else:
                NAICS_code_id = self.obIngester.ingest_naics_code(row['NAICSCode'])

        return NAICS_code_id

    def process_national_drug_code(self,row):
        national_drug_code_id = -1
        if 'NationalDrugCodeId' in row:
            national_drug_code_id = row['NationalDrugCodeId']

        return national_drug_code_id

    def process_product_warranty(self, row):
        product_warranty_id = -1
        if 'ProductWarrantyId' in row:
            product_warranty_id = row['ProductWarrantyId']

        return product_warranty_id

    def process_species(self, row):
        species_id = -1
        if 'SpeciesId' in row:
            species_id = row['SpeciesId']
        elif 'SpeciesScientificName' in row:
            species_sci_name = row['SpeciesScientificName']
            if 'SpeciesName' in row:
                species_name = row['SpeciesName']
                species_id = self.obIngester.ingest_species(species_sci_name,species_name)
            else:
                species_id = self.obIngester.ingest_species(species_sci_name)

        return species_id

    def process_image(self, row, product_id):
        if 'ImageName' in row:
            image_name = str(row['ImageName'])
            image_id = 1

            if (image_name in self.df_image_names['ProductImageName'].unique()):
                try:
                    image_id = int(self.df_image_names.loc[(self.df_image_names['ProductImageName']==image_name),['ProductImageSizeId']].values[0])
                except IndexError:
                    image_id = 1
                    self.obReporter.update_report('Alert','This image must be imported first.')
            else:
                image_id = 1

            if image_id != -1:
                # check image name against db names
                # get image id if any

                image_pref = 0
                if 'ImagePrefence' in row:
                    image_pref = int(row['ImagePrefence'])

                image_caption = ''
                if 'ImageCaption' in row:
                    image_caption = str(row['ImageCaption'])

                # i need to figure out how to handle shipping this sort of thing in short run
                # for now it just is true each time
                self.obIngester.image_cap(True, product_id, image_id, image_pref, image_caption)


class UpdateFillProduct(FillProduct):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerPartNumber','VendorPartNumber']
    sup_fields = []
    att_fields = ['Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = []

    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Update Product Fill'
