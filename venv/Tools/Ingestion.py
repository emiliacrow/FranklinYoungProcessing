# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20210813
# CreateFor: Franklin Young International

from Tools.ProgressBar import ProgressBarWindow


class IngestionObject:
    def __init__(self, obDal):
        self.name = 'Ingester Nester'
        self.obDal = obDal
        self.load_limit = 250
        self.base_price_collector = []
        self.product_collector = []
        self.product_price_collector = []
        self.product_image_match_collector = []

    def set_progress_bar(self, name, count_of_steps):
        self.obProgressBarWindow = ProgressBarWindow(name)
        self.obProgressBarWindow.show()
        self.obProgressBarWindow.set_anew(count_of_steps)


    def ingest_attribute(self,attribute_desc,table_name):
        return_id = self.obDal.attribute_cap(attribute_desc,table_name)
        return return_id

    def ingest_categories(self, df_categories):
        ingested_set = []
        self.set_progress_bar('Ingesting Categories',len(df_categories.index))
        p_bar = 0

        for column, row in df_categories.iterrows():
            category_string = str(row['Category'])
            category_name = category_string.rpartition('/')[2]

            return_id = self.obDal.category_cap(category_name, category_string)

            ingested_set.append([return_id,category_string])

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_product_category(self, product_id, category_id):
        # this takes a parent child and get the id of the category
        product_category_id = self.obDal.product_category_cap(product_id,category_id)
        return product_category_id

    def get_category_names(self):
        df_category_lookup = self.obDal.get_category_names()
        return df_category_lookup

    def ingest_country(self,country,country_long_name,country_code,ecatcode):
        # this should do more than just pass through
        return_id = self.obDal.country_cap(country,country_long_name,country_code,ecatcode)
        return return_id

    def ingest_countries(self,df_countries):
        ingested_set = []
        self.set_progress_bar('Ingesting Countries',len(df_countries.index))
        p_bar = 0

        for column, row in df_countries.iterrows():
            country = str(row['CountryName'])
            country_long_name = str(row['CountryLongName'])
            country_code = str(row['CountryCode'])
            ecatcode = str(row['ECATCountryCode'])
            return_id = self.ingest_country(country,country_long_name,country_code,ecatcode)
            ingested_set.append([return_id,country,country_long_name,country_code,ecatcode])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set

    def get_country_lookup(self):
        df_country_lookup = self.obDal.get_country_lookup()
        return df_country_lookup

    def ingest_vendor(self, vendor_name, vendor_code):
        return_id = self.obDal.vendor_cap(vendor_name, vendor_code)
        return return_id

    def ingest_vendors(self, df_vendors):
        ingested_set = []

        self.set_progress_bar('Ingesting Vendors',len(df_vendors.index))
        p_bar = 0
        for column, row in df_vendors.iterrows():

            vendor_name = str(row['VendorName'])
            vendor_name = vendor_name.strip().lower()
            vendor_code = str(row['VendorCode'])

            return_id = self.ingest_vendor(vendor_name,vendor_code)
            ingested_set.append([return_id,vendor_name,vendor_code])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()
        return ingested_set

    def get_vendor_lookup(self):
        df_vendor_lookup = self.obDal.get_vendor_lookup()
        return df_vendor_lookup

    def ingest_manufacturer(self, manufacturer_name, supplier_name, manufacturer_prefix = -1):
        return_id = self.obDal.manufacturer_cap(manufacturer_name, supplier_name, manufacturer_prefix)
        return return_id

    def ingest_manufacturers(self,df_manufacturers):
        ingested_set = []
        self.set_progress_bar('Ingesting Manufacturers',len(df_manufacturers.index))
        p_bar = 0

        for column, row in df_manufacturers.iterrows():
            manufacturer_prefix = -1
            supplier_name = str(row['SupplierName'])
            supplier_name = supplier_name.strip().lower()
            manufacturer_name = str(row['ManufacturerName'])

            if (row['FYManufacturerPrefix'] != ''):
                manufacturer_prefix = int(row['FYManufacturerPrefix'])

            return_id = self.ingest_manufacturer(manufacturer_name,supplier_name,manufacturer_prefix)
            ingested_set.append([return_id,manufacturer_name,supplier_name,manufacturer_prefix])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set

    def get_manufacturer_lookup(self):
        df_manufacturer_lookup = self.obDal.get_manufacturer_lookup()
        return df_manufacturer_lookup

    def ingest_unspsc(self,unspsc_code,unspsc_title,unspsc_desc=''):
        return_id = self.obDal.unspsc_code_cap(unspsc_code,unspsc_title,unspsc_desc)
        return return_id

    def ingest_unspscs(self,df_unspscs):
        ingested_set = []
        self.set_progress_bar('Ingesting UNSPSCs',len(df_unspscs.index))
        p_bar = 0
        for column, row in df_unspscs.iterrows():
            unspsc_code = str(row['UNSPSCCode'])
            unspsc_title = str(row['UNSPSCName'])[:45]
            unspsc_desc = str(row['UNSPSCDesc'])

            return_id = self.ingest_unspsc(unspsc_code,unspsc_title,unspsc_desc)
            ingested_set.append([return_id,unspsc_code,unspsc_title,unspsc_desc])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_fsc(self,fsc_code,fsc_name,fsc_desc=''):
        return_id = self.obDal.fsc_code_cap(fsc_code,fsc_name,fsc_desc)
        return return_id

    def ingest_fscs(self,df_fscs):
        ingested_set = []
        self.set_progress_bar('Ingesting FSCs',len(df_fscs.index))
        p_bar = 0
        for column, row in df_fscs.iterrows():
            fsc_code = str(row['FSCCode'])
            fsc_name = str(row['FSCName'])
            fsc_desc = str(row['FSCDesc'])

            return_id = self.ingest_fsc(fsc_code,fsc_name,fsc_desc)
            ingested_set.append([return_id,fsc_code,fsc_name,fsc_desc])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_hazard_code(self, hazard_code, hazard_desc, hazard_cat=''):
        return_id = self.obDal.hazardous_code_cap(hazard_code, hazard_desc, hazard_cat)
        return return_id

    def ingest_hazard_codes(self, df_hazard_codes):
        ingested_set = []
        self.set_progress_bar('Ingesting Hazard Codes',len(df_hazard_codes.index))
        p_bar = 0
        for column, row in df_hazard_codes.iterrows():
            hazard_code = str(row['HazardousCode'])
            hazard_desc = str(row['HazDesc'])

            return_id = self.ingest_hazard_code(hazard_code, hazard_desc)
            ingested_set.append([return_id, hazard_code, hazard_desc])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_naics_code(self, naics_code, naics_name=''):
        return_id = self.obDal.naics_code_cap(naics_code, naics_name)
        return return_id

    def ingest_naics_codes(self, df_naics_codes):
        ingested_set = []
        self.set_progress_bar('Ingesting NAICS Codes',len(df_naics_codes.index))
        p_bar = 0
        for column, row in df_naics_codes.iterrows():
            naics_code = str(row['NAICSCode'])
            naics_name = str(row['NAICSName'])

            return_id = self.ingest_naics_code(naics_code, naics_name)
            ingested_set.append([return_id, naics_code, naics_name])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_uoi_symbol(self, uoi_2_char, ecat_uoi = '', uoi_name = ''):
        return_id = self.obDal.unit_of_issue_symbol_cap(uoi_2_char, uoi_name, ecat_uoi)
        return return_id

    def ingest_uoi_symbols(self, df_uois):
        ingested_set = []
        self.set_progress_bar('Ingesting UOIs Codes',len(df_uois.index))
        p_bar = 0
        for column, row in df_uois.iterrows():
            ecat_uoi = str(row['ECAT UOI'])
            uoi_2_char = str(row['2-Char UOI'])
            uoi_name = str(row['UOI Name'])

            return_id = self.ingest_uoi_symbol(uoi_2_char, ecat_uoi, uoi_name)
            ingested_set.append([return_id, uoi_2_char, ecat_uoi, uoi_name])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set

    def ingest_uoi(self, unit_of_issue_id, count, unit_of_measure_id):
        return_id = self.obDal.unit_of_issue_cap(unit_of_issue_id, count, unit_of_measure_id)
        return return_id

    def ingest_uoi_by_symbol(self, unit_of_issue, count, unit_of_measure):
        return_id = -1
        unit_of_issue_id = self.ingest_uoi_symbol(unit_of_issue)
        unit_of_measure_id = self.ingest_uoi_symbol(unit_of_measure)
        if(unit_of_issue_id != -1 and unit_of_measure_id != -1):
            return_id = self.obDal.unit_of_issue_cap(unit_of_issue_id, count, unit_of_measure_id)
        return return_id

    def ingest_uois(self, df_uois):
        ingested_set = []
        self.set_progress_bar('Ingesting UOIs',len(df_uois.index))
        p_bar = 0
        for column, row in df_uois.iterrows():
            return_id = -1
            unit_of_issue = str(row['UnitOfIssue'])
            count = int(row['Count'])
            unit_of_measure = str(row['UnitOfMeasure'])
            if(count > 0):
                return_id = self.ingest_uoi_by_symbol(unit_of_issue, count, unit_of_measure)
            ingested_set.append([return_id, unit_of_issue, count, unit_of_measure])

            p_bar += 1
            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        return ingested_set


    def ingest_product(self, is_last, newFYCatalogNumber, newManufacturerPartNumber, newProductName, newShortDescription, newLongDescription, newECommerceLongDescription, newCountryOfOriginId, newManufacturerId, newShippingInstructionsId, newRecommendedStorageId, newExpectedLeadTimeId, newCategoryId, newIsControlled=0, newIsDisposable=0, newIsGreen=0, newIsLatexFree=0, newIsRX=0):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([newFYCatalogNumber, newManufacturerPartNumber, newProductName, newShortDescription,
                                               newLongDescription, newECommerceLongDescription,
                                               newCountryOfOriginId, newManufacturerId, newShippingInstructionsId,
                                               newRecommendedStorageId, newExpectedLeadTimeId, newCategoryId, newIsControlled,
                                               newIsDisposable, newIsGreen, newIsLatexFree, newIsRX])

            self.obDal.min_product_cap(self.product_collector)
            self.product_collector = []

        else:
            self.product_collector.append([newFYCatalogNumber, newManufacturerPartNumber, newProductName, newShortDescription,
                                               newLongDescription, newECommerceLongDescription,
                                               newCountryOfOriginId, newManufacturerId, newShippingInstructionsId,
                                               newRecommendedStorageId, newExpectedLeadTimeId, newCategoryId, newIsControlled,
                                               newIsDisposable, newIsGreen, newIsLatexFree, newIsRX])

    def ingest_product_cleanup(self):
        if self.product_collector != []:
            self.obDal.min_product_cap(self.product_collector)


    def get_product_id_by_fy_catalog_number(self, fy_catalog_number):
        return_id = -1
        return_id = self.obDal.get_product_id_by_fy_catalog_number(fy_catalog_number)
        return return_id

    def get_product_id_by_manufacturer_part_number(self, manufacturer_part_number):
        return_id = -1
        return_id = self.obDal.get_product_id_by_manufacturer_part_number(manufacturer_part_number)
        return return_id


    def fill_product(self, is_last, ProductId, FYProductNotes='', NatoStockNumber='', ModelNumber='', RequiredSampleSize='', NumberOfChannels='', GTIN='', SterilityId=-1, SurfaceTreatmentId=-1, PrecisionId=-1, ProductSEOId=-1, ComponentSetId=-1, FSCCodeId=-1, HazardousCodeId=-1, UNSPSCId=-1, NAICSCodeId=-1, NationalDrugCodeId=-1, ProductWarrantyId=-1, SpeciesId=-1):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([ProductId, FYProductNotes, NatoStockNumber, ModelNumber, RequiredSampleSize, NumberOfChannels, GTIN, SterilityId, SurfaceTreatmentId, PrecisionId, ProductSEOId, ComponentSetId, FSCCodeId, HazardousCodeId, UNSPSCId, NAICSCodeId, NationalDrugCodeId, ProductWarrantyId, SpeciesId])
            self.obDal.product_fill(self.product_collector)
            self.product_collector = []

        else:
            self.product_collector.append([ProductId, FYProductNotes, NatoStockNumber, ModelNumber, RequiredSampleSize, NumberOfChannels, GTIN, SterilityId, SurfaceTreatmentId, PrecisionId, ProductSEOId, ComponentSetId, FSCCodeId, HazardousCodeId, UNSPSCId, NAICSCodeId, NationalDrugCodeId, ProductWarrantyId, SpeciesId])

    def fill_product_cleanup(self):
        if self.product_collector != []:
            self.obDal.product_fill(self.product_collector)


    def ingest_shipping_instructions(self, newPackagingDesc, newshippingcode='', newIsFreeShipping=0, newIsColdChain=0):
        return_id = self.obDal.shipping_instructions_cap(newPackagingDesc, newshippingcode, newIsFreeShipping, newIsColdChain)
        return return_id

    def ingest_expected_lead_times(self, expected_lead_time,expedited_lead_time=-1):
        if (expedited_lead_time == -1):
            expedited_lead_time = expected_lead_time
        return_id = self.obDal.lead_time_cap(expected_lead_time,expedited_lead_time)
        return return_id

    def get_fsc_id(self,fsc_code,fsc_code_name=''):
        return_id = self.obDal.get_fsc_id(fsc_code,fsc_code_name)
        return return_id

    def get_unspsc_id(self, unspsc_code,unspsc_code_title=''):
        return_id = self.obDal.get_unspsc_id(unspsc_code,unspsc_code_title)
        return return_id

    def ingest_species(self,species_sci_name, species_name = ''):
        return_id = self.obDal.species_cap(species_sci_name,species_name)
        return return_id


    def ingest_product_price(self, is_last, newFyProductNumber,newAllowPurchases,newFyPartNumber,newProductTaxClass,newVendorPartNumber,newProductId,newVendorId,newUnitOfIssueSymbolId,newUnitOfMeasureSymbolId,newUnitOfIssueQuantity):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([newFyProductNumber,newAllowPurchases,newFyPartNumber,newProductTaxClass,newVendorPartNumber,newProductId,newVendorId,newUnitOfIssueSymbolId,newUnitOfMeasureSymbolId,newUnitOfIssueQuantity])

            self.obDal.min_product_price_cap(self.product_collector)
            self.product_collector = []

        else:
            self.product_collector.append([newFyProductNumber, newAllowPurchases, newFyPartNumber,
                                                newProductTaxClass, newVendorPartNumber, newProductId, newVendorId,
                                                newUnitOfIssueSymbolId,newUnitOfMeasureSymbolId,newUnitOfIssueQuantity])

    def ingest_product_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.min_product_price_cap(self.product_collector)

    def fill_product_price(self, is_last, newProductPriceId,newUPC='',newVolume=-1,newWeight=-1,newSize='',newLength=-1,newVariantDesc='',newMinumumFlowTime='',newProfile='',newAmountPriceBreakLevel1=-1,newAmountPriceBreakLevel2=-1,newAmountPriceBreakLevel3=-1,newQuantityPriceBreakLevel1=-1,newQuantityPriceBreakLevel2=-1,newQuantityPriceBreakLevel3=-1,newThicknessId=-1,newHeightId=-1,newDepthId=-1,newWidthId=-1,newCapacityId=-1,newTankCapacityId=-1,newVolumeUnitId=-1,newWeightUnitId=-1,newLengthUnitId=-1,newDimensionsId=-1,newInteriorDimensionsId=-1,newExteriorDimensionsId=-1,newMaterialId=-1,newColorId=-1,newSpeedId=-1,newTubeId=-1,newWeightRangeId=-1,newTemperatureRangeId=-1,newWavelengthId=-1,newWattageId=-1,newVoltageId=-1,newAmperageId=-1,newOuterDiameterId=-1,newInnerDiameterId=-1,newDiameterId=-1,newToleranceId=-1,newAccuracyId=-1,newMassId=-1,newApertureSizeId=-1,newApparelSizeId=-1,newParticleSizeId=-1,newPoreSizeId=-1):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([newProductPriceId,newUPC,newVolume,newWeight,newSize,newLength,newVariantDesc,newMinumumFlowTime,newProfile,newAmountPriceBreakLevel1,newAmountPriceBreakLevel2,newAmountPriceBreakLevel3,newQuantityPriceBreakLevel1,newQuantityPriceBreakLevel2,newQuantityPriceBreakLevel3,newThicknessId,newHeightId,newDepthId,newWidthId,newCapacityId,newTankCapacityId,newVolumeUnitId,newWeightUnitId,newLengthUnitId,newDimensionsId,newInteriorDimensionsId,newExteriorDimensionsId,newMaterialId,newColorId,newSpeedId,newTubeId,newWeightRangeId,newTemperatureRangeId,newWavelengthId,newWattageId,newVoltageId,newAmperageId,newOuterDiameterId,newInnerDiameterId,newDiameterId,newToleranceId,newAccuracyId,newMassId,newApertureSizeId,newApparelSizeId,newParticleSizeId,newPoreSizeId])
            self.obDal.product_price_fill(self.product_collector)
            self.product_collector = []

        else:
            self.product_collector.append([newProductPriceId,newUPC,newVolume,newWeight,newSize,newLength,newVariantDesc,newMinumumFlowTime,newProfile,newAmountPriceBreakLevel1,newAmountPriceBreakLevel2,newAmountPriceBreakLevel3,newQuantityPriceBreakLevel1,newQuantityPriceBreakLevel2,newQuantityPriceBreakLevel3,newThicknessId,newHeightId,newDepthId,newWidthId,newCapacityId,newTankCapacityId,newVolumeUnitId,newWeightUnitId,newLengthUnitId,newDimensionsId,newInteriorDimensionsId,newExteriorDimensionsId,newMaterialId,newColorId,newSpeedId,newTubeId,newWeightRangeId,newTemperatureRangeId,newWavelengthId,newWattageId,newVoltageId,newAmperageId,newOuterDiameterId,newInnerDiameterId,newDiameterId,newToleranceId,newAccuracyId,newMassId,newApertureSizeId,newApparelSizeId,newParticleSizeId,newPoreSizeId])

    def fill_product_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.product_price_fill(self.product_collector)

    def ingest_base_price(self, is_last, vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price, ecommerce_discount, is_visible, date_catalog_recieved, catalog_provided_by, product_price_id, newVAProductPriceId=-1, newGSAProductPriceId=-1, newHTMEProductPriceId=-1, newECATProductPriceId=-1, newFEDMALLProductPriceId=-1):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append(
                [vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost,
                 markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price, ecommerce_discount,
                 is_visible, date_catalog_recieved, catalog_provided_by, product_price_id,
                 newVAProductPriceId, newGSAProductPriceId, newHTMEProductPriceId, newECATProductPriceId,
                 newFEDMALLProductPriceId])
            self.obDal.base_price_cap(self.product_collector)
            self.product_collector = []
        else:
            self.product_collector.append([vendor_list_price, fy_discount_percent, fy_cost, estimated_freight, fy_landed_cost, markup_percent_fy_sell, fy_sell_price, markup_percent_fy_list, fy_list_price, ecommerce_discount, is_visible, date_catalog_recieved, catalog_provided_by, product_price_id, newVAProductPriceId, newGSAProductPriceId, newHTMEProductPriceId, newECATProductPriceId, newFEDMALLProductPriceId])

    def ingest_base_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.base_price_cap(self.product_collector)


    def get_product_price_id_by_fy_product_number(self,fy_product_number):
        success = self.obDal.get_product_price_id_by_fy_product_number(fy_product_number)
        return success

    def get_product_price_id_by_fy_part_number(self,fy_part_number):
        success = self.obDal.get_product_price_id_by_fy_part_number(fy_part_number)
        return success

    def get_product_price_id_by_vendor_part_number(self,vendor_part_number):
        success = self.obDal.get_product_price_id_by_vendor_part_number(vendor_part_number)
        return success


    def gsa_product_price_cap(self, is_last, newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber, newContractModificatactionNumber, newGSAPricingApproved, newGSAApprovedPriceDate,newApprovedListPrice, newApprovedPercent, newGSABasePrice, newGSASellPrice, newMFCPercent, newMFCPrice, newGSA_SIN):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append(
                [newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber, newContractModificatactionNumber, newGSAPricingApproved, newGSAApprovedPriceDate,newApprovedListPrice, newApprovedPercent, newGSABasePrice, newGSASellPrice, newMFCPercent, newMFCPrice, newGSA_SIN])
            self.obDal.gsa_product_price_cap(self.product_collector)
            self.product_collector = []
        else:
            self.product_collector.append([newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber, newContractModificatactionNumber, newGSAPricingApproved, newGSAApprovedPriceDate,newApprovedListPrice, newApprovedPercent, newGSABasePrice, newGSASellPrice, newMFCPercent, newMFCPrice, newGSA_SIN])

    def ingest_gsa_product_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.gsa_product_price_cap(self.product_collector)


    def va_product_price_cap(self, is_last, newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber, newContractModificatactionNumber, newVAPricingApproved, newVAApprovedPriceDate,newApprovedListPrice, newApprovedPercent, newVABasePrice, newVASellPrice, newMFCPercent, newMFCPrice, newVA_SIN):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append(
                [newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber,
                 newContractModificatactionNumber, newVAPricingApproved, newVAApprovedPriceDate, newApprovedListPrice,
                 newApprovedPercent, newVABasePrice, newVASellPrice, newMFCPercent, newMFCPrice, newVA_SIN])
            self.obDal.va_product_price_cap(self.product_collector)
            self.product_collector = []
        else:
            self.product_collector.append(
                [newBaseProductPriceId, newFyProductNumber, newOnContract, newContractNumber,
                 newContractModificatactionNumber, newVAPricingApproved, newVAApprovedPriceDate, newApprovedListPrice,
                 newApprovedPercent, newVABasePrice, newVASellPrice, newMFCPercent, newMFCPrice, newVA_SIN])

    def ingest_va_product_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.va_product_price_cap(self.product_collector)

    def ecat_product_price_cap(self, is_last, newBaseProductPriceId, newFyProductNumber, newOnContract, newApprovedListPrice, newContractNumber, newContractModificatactionNumber, newECATPricingApproved, newECATApprovedPriceDate, newApprovedPercent, newECATBasePrice, newECATSellPrice, newMFCPercent, newMFCPrice):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append(
                [newBaseProductPriceId, newFyProductNumber, newOnContract, newApprovedListPrice, newContractNumber, newContractModificatactionNumber, newECATPricingApproved, newECATApprovedPriceDate, newApprovedPercent, newECATBasePrice, newECATSellPrice, newMFCPercent, newMFCPrice])
            self.obDal.ecat_product_price_cap(self.product_collector)
            self.product_collector = []
        else:
            self.product_collector.append([newBaseProductPriceId, newFyProductNumber, newOnContract, newApprovedListPrice, newContractNumber, newContractModificatactionNumber, newECATPricingApproved, newECATApprovedPriceDate, newApprovedPercent, newECATBasePrice, newECATSellPrice, newMFCPercent, newMFCPrice])

    def ingest_ecat_product_price_cleanup(self):
        if self.product_collector != []:
            self.obDal.ecat_product_price_cap(self.product_collector)

    def htme_product_price_cap(self,newIsVisible, newDateCatalogReceived, newHTMESellPrice, newHTMEApprovedPriceDate, newHTMEPricingApproved, newHTMEContractNumber, newHTMEContractModificationNumber, newHTMEProductGMPercent, newHTMEProductGMPrice):
        return_id = self.obDal.htme_product_price_cap(newIsVisible, newDateCatalogReceived, newHTMESellPrice, newHTMEApprovedPriceDate, newHTMEPricingApproved, newHTMEContractNumber, newHTMEContractModificationNumber, newHTMEProductGMPercent, newHTMEProductGMPrice)
        return return_id


    def fedmall_product_price_cap(self,newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate , newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice):
        return_id = self.obDal.fedmall_product_price_cap(newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate, newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice)
        return return_id

    def image_size_cap(self, is_last, newProductImageUrl, newProductImageName, newProductImageX, newProductImageY):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([newProductImageUrl, newProductImageName, newProductImageX, newProductImageY])
            self.obDal.image_size_cap(self.product_collector)
            self.product_collector = []
        else:
            self.product_collector.append([newProductImageUrl, newProductImageName, newProductImageX, newProductImageY])


    def image_cap(self, is_last, product_id, image_id, image_preference = 0, image_caption = ''):
        if (len(self.product_image_match_collector) >= self.load_limit) or (is_last):
            self.product_image_match_collector.append([product_id,image_id,image_preference,image_caption])
            self.obDal.image_cap(self.product_image_match_collector)
            self.product_image_match_collector = []
        else:
            self.product_image_match_collector.append([product_id,image_id,image_preference,image_caption])


    def set_bigcommerce_rtl(self, is_last, ProductPriceId, FyProductNumber, PriceTrigger, DataTrigger):
        if (len(self.product_collector) >= self.load_limit) or (is_last):
            self.product_collector.append([ProductPriceId, FyProductNumber, PriceTrigger, DataTrigger])
            self.obDal.set_bc_rtl(self.product_collector)
            self.product_collector = []

        else:
            self.product_collector.append([ProductPriceId, FyProductNumber, PriceTrigger, DataTrigger])


## end ##