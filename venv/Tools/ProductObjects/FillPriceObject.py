# CreatedBy: Emilia Crow
# CreateDate: 20210528
# Updated: 20210528
# CreateFor: Franklin Young International

import pandas

from Tools.BasicProcess import BasicProcessObject


class FillProductPrice(BasicProcessObject):
    req_fields = ['FyProductNumber','VendorName']
    att_fields = ['Accuracy', 'Amperage', 'ApertureSize', 'ApparelSize', 'Capacity', 'Color', 'Component',
                          'Depth', 'Diameter', 'Dimensions', 'ExteriorDimensions', 'Height', 'InnerDiameter',
                          'InteriorDimensions', 'Mass', 'Material', 'OuterDiameter', 'ParticleSize', 'PoreSize',
                          'Speed', 'TankCapacity', 'TemperatureRange', 'Thickness', 'Tolerance', 'Voltage', 'Wattage',
                          'Wavelength', 'WeightRange', 'Width']
    sup_fields = []
    gen_fields = []

    def __init__(self,df_product, is_testing):
        super().__init__(df_product, is_testing)
        self.name = 'Product Price Fill'

    def batch_preprocessing(self):
        # ident product price id
        self.batch_process_vendor()
        self.define_new()
        # this is here to consume values that may be more alike than different
        # for example, recommended storage might have a lot of 'keep this darn thing cold'
        for each_attribute in self.att_fields:
            if each_attribute in self.df_product.columns:
                self.batch_process_attribute(each_attribute)

        return self.df_product

    def batch_process_vendor(self):
        # there should only be one vendor, really.
        df_attribute = self.df_product[['VendorName']]
        df_attribute = df_attribute.drop_duplicates(subset=['VendorName'])
        lst_ids = []
        if 'VendorId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'VendorId')

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
        self.df_base_price_lookup = self.obDal.get_base_product_price_lookup_by_vendor_id(lst_ids[0])

        self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                 how='left', on=['VendorName'])


    def define_new(self):
        match_headers = ['FyProductNumber','ProductPriceId','Vendor List Price', 'Discount', 'Fy Cost', 'Fixed Shipping Cost', 'LandedCostMarkupPercent_FYSell']

        # simple first
        self.df_base_price_lookup['Filter'] = 'Update'
        self.df_base_price_check_in = self.df_base_price_lookup[['FyProductNumber','ProductPriceId','Filter']]

        if 'Filter' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'Filter')
        if 'ProductPriceId' in self.df_product.columns:
            self.df_product = self.df_product.drop(columns = 'ProductPriceId')

        # match all products on FyProdNum
        self.df_update_products = pandas.DataFrame.merge(self.df_product, self.df_base_price_check_in,
                                                 how='left', on='FyProductNumber')
        # all products that matched on FyProdNum
        self.df_update_products.loc[(self.df_update_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        self.df_product = self.df_update_products


    def batch_process_attribute(self,attribute):
        str_attribute_id = attribute +'Id'
        if str_attribute_id not in self.df_product.columns:
            df_attribute = self.df_product[[attribute,str_attribute_id]]
            df_attribute = df_attribute.drop_duplicates(subset=[attribute,str_attribute_id])
            lst_ids = []
            for colName, row in df_attribute.iterrows():
                attribute_phrase = row[attribute]
                if len(attribute_phrase) > 128:
                    attribute_phrase = attribute_phrase[:128]

                attribute_id = self.obIngester.ingest_attribute(attribute_phrase, attribute)
                lst_ids.append(attribute_id)

            df_attribute[str_attribute_id] = lst_ids

            self.df_product = pandas.DataFrame.merge(self.df_product, df_attribute,
                                                              how='left', on=[attribute])


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()
        # step-wise product processing
        for colName, row in df_line_product.iterrows():
            if 'Filter' in row:
                if row['Filter'] == 'Pass':
                    return True, df_collect_product_base_data
                elif row['Filter'] != 'Update':
                    self.obReporter.update_report('Fail','This product must be ingested in product price')
                    return True, df_collect_product_base_data
            else:
                self.obReporter.update_report('Fail','This product must be ingested in product price')
                return False, df_collect_product_base_data

            if ('ProductPriceId' not in row):
                self.obReporter.update_report('Fail','ProductPriceId is missing')
                return False, df_collect_product_base_data

            if ('LongDescription' in row):
                success, df_collect_product_base_data = self.process_long_desc(df_collect_product_base_data, row)
                if success == False:
                    self.obReporter.update_report('Fail','LongDescription failed in attribute extraction')
                    return success, df_collect_product_base_data

        df_line_product = self.process_attribute_data(df_collect_product_base_data)

        self.fill_product_price_data(df_line_product)
        return True, df_collect_product_base_data


    def process_long_desc(self, df_collect_product_base_data, row):
        long_desc = row['LongDescription']

        # extraction
        if 'Dimensions' not in row:
            # maybe we should be checking if the match object hits first
            # we could clean up any dimension info if it hits
            long_desc, dimensions = self.obExtractor.extract_dimensions(long_desc)
            df_collect_product_base_data['Dimensions'] = dimensions
            # add else standardize

        # rinse and repeat


        return True, df_collect_product_base_data

    def fill_product_price_data(self, df_line_product):
        amount_price_break_1 = 0
        amount_price_break_2 = 0
        amount_price_break_3 = 0

        quantity_price_break_1 = 0
        quantity_price_break_2 = 0
        quantity_price_break_3 = 0

        for colName, row in df_line_product.iterrows():
            product_price_id = row['ProductPriceId']
            upc = self.process_upc(row)
            volume, volume_unit_id = self.process_volume(row)
            weight, weight_unit_id = self.process_weight(row)
            length, length_unit_id = self.process_length(row)
            size = self.process_size(row)

            variant_desc = self.process_variant_desc(row)
            min_flow_time = self.process_min_flow_time(row)
            profile = self.process_profile(row)

            if 'AmountPriceBreakLevel1' in row:
                amount_price_break_1 = row['AmountPriceBreakLevel1']
            if 'AmountPriceBreakLevel2' in row:
                amount_price_break_1 = row['AmountPriceBreakLevel2']
            if 'AmountPriceBreakLevel3' in row:
                amount_price_break_1 = row['AmountPriceBreakLevel3']

            if 'QuantityPriceBreakLevel1' in row:
                amount_price_break_1 = row['QuantityPriceBreakLevel1']
            if 'QuantityPriceBreakLevel2' in row:
                amount_price_break_1 = row['QuantityPriceBreakLevel2']
            if 'QuantityPriceBreakLevel3' in row:
                amount_price_break_1 = row['QuantityPriceBreakLevel3']

            thickness_id = self.process_thickness(row)
            height_id = self.process_height(row)
            depth_id = self.process_depth(row)
            width_id = self.process_width(row)
            weight_range_id = self.process_weight_range(row)
            temperature_range_id = self.process_temperature_range(row)

            dimensions_id = self.process_dimensions(row)
            interior_dimensions_id = self.process_interior_dimensions(row)
            exterior_dimensions_id = self.process_exterior_dimensions(row)

            capacity_id = self.process_capacity(row)
            tank_capacity_id = self.process_tank_capacity(row)

            material_id = self.process_material(row)
            color_id = self.process_color(row)
            speed_id = self.process_speed(row)
            tube_id = self.process_tube(row)

            wavelength_id = self.process_wavelength(row)
            wattage_id = self.process_wattage(row)
            voltage_id = self.process_voltage(row)
            amperage_id = self.process_amperage(row)

            diameter_id = self.process_diameter(row)
            outer_diameter_id = self.process_outer_diameter(row)
            inner_diameter_id = self.process_inner_diameter(row)

            tolerance_id = self.process_tolerance(row)
            accuracy_id = self.process_accuracy(row)
            mass_id = self.process_mass(row)

            aperture_size_id = self.process_aperture_size(row)
            apparel_size_id = self.process_apparel_size(row)
            particle_size_id = self.process_particle_size(row)
            pore_size_id = self.process_pore_size(row)

        self.obIngester.fill_product_price(self.is_last, product_price_id, upc, volume, weight, size,
                                                     length, variant_desc, min_flow_time, profile,
                                                     quantity_price_break_1, quantity_price_break_2,
                                                     quantity_price_break_3,
                                                     amount_price_break_1, amount_price_break_2, amount_price_break_3,
                                                     thickness_id, height_id, depth_id, width_id, capacity_id,
                                                     tank_capacity_id, volume_unit_id, weight_unit_id,
                                                     length_unit_id, dimensions_id, interior_dimensions_id,
                                                     exterior_dimensions_id, material_id, color_id, speed_id,
                                                     tube_id, weight_range_id, temperature_range_id,
                                                     wavelength_id, wattage_id, voltage_id, amperage_id,
                                                     outer_diameter_id, inner_diameter_id, diameter_id,
                                                     tolerance_id, accuracy_id, mass_id, aperture_size_id,
                                                     apparel_size_id, particle_size_id, pore_size_id)


    def process_upc(self,row):
        upc = ''
        if 'UPC' in row:
            upc = row['UPC']
        return upc

    def process_volume(self, row):
        volume = -1
        volume_unit_id = -1
        return volume, volume_unit_id

    def process_weight(self, row):
        weight = -1
        weight_unit_id = -1
        return weight, weight_unit_id

    def process_length(self, row):
        length = -1
        length_unit = -1
        return length, length_unit

    def process_size(self, row):
        size = ''
        if ('Size' in row):
            size = row['Size']
        return size

    def process_variant_desc(self, row):
        variant_desc = ''
        if ('VariantDesc' in row):
            variant_desc = row['VariantDesc']
        return variant_desc

    def process_min_flow_time(self, row):
        min_flow_time = ''
        if ('MinimumFlowTime' in row):
            min_flow_time = row['MinimumFlowTime']
        return min_flow_time

    def process_profile(self, row):
        profile = ''
        if ('Profile' in row):
            profile = row['Profile']
        return profile


    def process_thickness(self, row):
        thickness_id = -1
        if ('ThicknessId' in row):
            thickness_id = row['ThicknessId']
        return thickness_id

    def process_height(self, row):
        height_id = -1
        if ('HeightId' in row):
            height_id = row['HeightId']
        return height_id

    def process_depth(self, row):
        depth_id = -1
        if ('DepthId' in row):
            depth_id = row['DepthId']
        return depth_id

    def process_width(self, row):
        width_id = -1
        if ('WidthId' in row):
            width_id = row['WidthId']
        return width_id

    def process_weight_range(self, row):
        weight_range_id = -1
        if ('WeightRangeId' in row):
            weight_range_id = row['WeightRangeId']
        return weight_range_id

    def process_temperature_range(self, row):
        temperature_range_id = -1
        if ('TemperatureRangeId' in row):
            temperature_range_id = row['TemperatureRangeId']
        return temperature_range_id


    def process_dimensions(self, row):
        dimensions_id = -1
        if ('DimensionsId' in row):
            dimensions_id = row['DimensionsId']
        return dimensions_id

    def process_interior_dimensions(self, row):
        interior_dimensions_id = -1
        if ('InteriorDimensionsId' in row):
            interior_dimensions_id = row['InteriorDimensionsId']
        return interior_dimensions_id

    def process_exterior_dimensions(self, row):
        exterior_dimensions_id = -1
        if ('ExteriorDimensionsId' in row):
            exterior_dimensions_id = row['ExteriorDimensionsId']
        return exterior_dimensions_id



    def process_capacity(self, row):
        capacity_id = -1
        if ('CapacityId' in row):
            capacity_id = row['CapacityId']
        return capacity_id

    def process_tank_capacity(self, row):
        tank_capacity_id = -1
        if ('TankCapacityId' in row):
            tank_capacity_id = row['TankCapacityId']
        return tank_capacity_id


    def process_material(self, row):
        material_id = -1
        if ('MaterialId' in row):
            material_id = row['MaterialId']
        return material_id

    def process_color(self, row):
        color_id = -1
        if ('ColorId' in row):
            color_id = row['ColorId']
        return color_id

    def process_speed(self, row):
        speed_id = -1
        if ('SpeedId' in row):
            speed_id = row['SpeedId']
        return speed_id

    def process_tube(self, row):
        tube_id = -1
        if ('TubeId' in row):
            tube_id = row['TubeId']
        return tube_id


    def process_wavelength(self, row):
        wavelength_id = -1
        if ('WavelengthId' in row):
            wavelength_id = row['WavelengthId']
        return wavelength_id

    def process_wattage(self, row):
        wattage_id = -1
        if ('WattageId' in row):
            wattage_id = row['WattageId']
        return wattage_id

    def process_voltage(self, row):
        voltage_id = -1
        if ('VoltageId' in row):
            voltage_id = row['VoltageId']
        return voltage_id

    def process_amperage(self, row):
        amperage_id = -1
        if ('AmperageId' in row):
            amperage_id = row['AmperageId']
        return amperage_id


    def process_diameter(self, row):
        diameter_id = -1
        if ('DiameterId' in row):
            diameter_id = row['DiameterId']
        return diameter_id

    def process_outer_diameter(self, row):
        outer_diameter_id = -1
        if ('OuterDiameterId' in row):
            outer_diameter_id = row['OuterDiameterId']
        return outer_diameter_id

    def process_inner_diameter(self, row):
        inner_diameter_id = -1
        if ('InnerDiameterId' in row):
            inner_diameter_id = row['InnerDiameterId']
        return inner_diameter_id


    def process_tolerance(self, row):
        tolerance_id = -1
        if ('ToleranceId' in row):
            tolerance_id = row['ToleranceId']
        return tolerance_id

    def process_accuracy(self, row):
        accuracy_id = -1
        if ('AccuracyId' in row):
            accuracy_id = row['AccuracyId']
        return accuracy_id

    def process_mass(self, row):
        mass_id = -1
        if ('MassId' in row):
            mass_id = row['MassId']
        return mass_id


    def process_aperture_size(self, row):
        aperture_size_id = -1
        if ('ApertureSizeId' in row):
            aperture_size_id = row['ApertureSizeId']
        return aperture_size_id

    def process_apparel_size(self, row):
        apparel_size_id = -1
        if ('ApparelSizeId' in row):
            apparel_size_id = row['ApparelSizeId']
        return apparel_size_id

    def process_particle_size(self, row):
        particle_size_id = -1
        if ('ParticleSizeId' in row):
            particle_size_id = row['ParticleSizeId']
        return particle_size_id

    def process_pore_size(self, row):
        pore_size_id = -1
        if ('PoreSizeId' in row):
            pore_size_id = row['PoreSizeId']
        return pore_size_id



