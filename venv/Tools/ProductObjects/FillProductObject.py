# CreatedBy: Emilia Crow
# CreateDate: 20210527
# Updated: 20210804
# CreateFor: Franklin Young International


import pandas
from Tools.BasicProcess import BasicProcessObject


class FillProduct(BasicProcessObject):
    req_fields = ['FyProductNumber','LongDescription','ShortDescription']
    sup_fields = ['FyCatalogNumber','ManufacturerPartNumber']
    att_fields = ['Sterility', 'SurfaceTreatment', 'Precision']
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Product Fill'


    def batch_preprocessing(self):
        self.define_new()

        # this is here to consume values that may be more alike than different
        # for example, recommended storage might have a lot of 'keep this darn thing cold'
        if 'Sterility' in self.df_product.columns:
            self.batch_process_attribute('Sterility')
        if 'SurfaceTreatment' in self.df_product.columns:
            self.batch_process_attribute('SurfaceTreatment')
        if 'Precision' in self.df_product.columns:
            self.batch_process_attribute('Precision')


    def define_new(self):
        # these are the df's for assigning data.
        self.df_product_lookup = self.obDal.get_product_lookup()
        self.df_product_full_lookup = self.obDal.get_product_fill_lookup()

        # if there's already a filter column, we remove it.
        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns=['Filter'])
        if 'ProductId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns=['ProductId'])
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns=['ProductPriceId'])

        self.df_product_lookup['Filter'] = 'Update'
        # match all products on FyProdNum and Manufacturer part, clearly
        if 'ManufacturerPartNumber' in self.df_product.columns:
            self.df_product = pandas.DataFrame.merge(self.df_product, self.df_product_lookup,
                                                            how='left',
                                                            on=['FyCatalogNumber', 'ManufacturerPartNumber'])
        else:
            self.df_product = pandas.DataFrame.merge(self.df_product, self.df_product_lookup,
                                                            how='left', on=['FyCatalogNumber'])

        # we assign a label to the products that are haven't been loaded through product yet
        self.df_product.loc[(self.df_product['Filter'] != 'Update'), 'Filter'] = 'Fail'

        # split the data for a moment
        self.df_update_product = self.df_product[(self.df_product['Filter'] == 'Update')]
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Update')]

        if len(self.df_update_product.index) != 0:
            self.df_product_full_lookup['Filter'] = 'Pass'
            # this section evaluates if these have product data loaded
            # drop some columns to ease processing
            if 'Filter' in self.df_update_product.columns:
                self.df_update_product = self.df_update_product.drop(columns=['Filter'])

            # this gets the productId
            self.df_update_product = pandas.DataFrame.merge(self.df_update_product, self.df_product_full_lookup,
                                                             how='left', on=['FyProductNumber','ShortDescription'])

            self.df_update_product.loc[(self.df_update_product['Filter'] != 'Pass'), 'Filter'] = 'Update'

            if 'ProductId_x' in self.df_update_product.columns:
                self.df_update_product['ProductId'] = self.df_update_product[['ProductId_x']]
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_x'])
                self.df_update_product = self.df_update_product.drop(columns=['ProductId_y'])

            if 'ProductPriceId_x' in self.df_update_product.columns:
                self.df_update_product['ProductPriceId'] = self.df_update_product[['ProductPriceId_x']]
                self.df_update_product = self.df_update_product.drop(columns=['ProductPriceId_x'])
                self.df_update_product = self.df_update_product.drop(columns=['ProductPriceId_y'])

            # recombine with product
            self.df_product = self.df_product.append(self.df_update_product)


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

    def process_product_line(self, df_line_product):
        df_collect_product_base_data = df_line_product.copy()
        df_collect_product_base_data = self.process_attribute_data(df_collect_product_base_data)

        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Pass':
                    return True, df_collect_product_base_data
                elif row['Filter'] != 'Update':
                    self.obReporter.update_report('Fail','This product must be imported')
                    return False, df_collect_product_base_data
            else:
                self.obReporter.update_report('Fail','This product must be imported')
                return False, df_collect_product_base_data

            if ('LongDescription' not in row):
                self.obReporter.update_report('Fail','LongDescription is missing')
                return False, df_collect_product_base_data

            if ('ShortDescription' not in row):
                self.obReporter.update_report('Fail','ShortDescription is missing')
                return False, df_collect_product_base_data

            product_id = row['ProductId']
            if product_id == -1:
                self.obReporter.update_report('Fail','Couldn\'t identify product id')
                return False, df_collect_product_base_data
            else:
                df_collect_product_base_data['ProductId'] = [product_id]

            self.process_image(row, product_id)

# this can be uncommented to consume categories
#            if 'Category' in row:
#                category = row['Category']
#                category_name = category.rpartition('/')[2]
#                try:
#                    category_id = self.df_category_names.loc[
#                        (self.df_category_names['CategoryName'] == category_name), 'CategoryId'].values[0]
#                except IndexError:
#                    category_name = category.rpartition('/')[2]
#                    category_id = self.obDal.category_cap(category_name, category)
#                    self.df_category_names.append([category_id,category_name,category])
#
#                category_insert = self.obIngester.ingest_product_category(product_id, category_id)

            long_desc = row['LongDescription']
            short_desc = row['ShortDescription']

            # we should separate the ones that require pre-processing steps from those that don't

            fy_product_Notes = self.process_fy_notes(row)
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

        self.obIngester.fill_product(self.is_last, product_id, long_desc, fy_product_Notes, short_desc, nato_stock_number, model_number,
                                               required_sample_size, number_of_channels, GTIN, sterility_id,
                                               surface_treatment_id, precision_id, product_seo_id, component_set_id,
                                               FSC_code_id, hazardous_code_id, UNSPSC_id, NAICS_code_id,
                                               national_drug_code_id, product_warranty_id, species_id)

        return True, df_collect_product_base_data


    def process_fy_notes(self, row):
        fy_product_Notes = ''
        if 'FyProductNotes' in row:
            fy_product_Notes = row['FyProductNotes']

        return fy_product_Notes

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
        if 'ImageUrl' in row:
            image_caption = ''
            is_video = 0
            image_pref = 0
            image_url = row['ImageUrl']
            if 'ImageCaption' in row:
                image_caption = row['ImageCaption']
            if 'ImagePreference' in row:
                image_pref = row['ImagePreference']
            if 'IsVideo' in row:
                is_video = row['IsVideo']

            success = self.obIngester.image_cap(image_url,image_caption,image_pref,is_video,product_id)



