# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20210813
# CreateFor: Franklin Young International


import time
import boto3
import pandas
import pymysql
import threading
import subprocess

from sqlalchemy import create_engine

class S3Object:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.name = 'Simple Stanley'
        self.region_name = 'us-west-2'
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def put_image(self, file_path, file_name, bucket, object_name = None):
        if object_name is None:
            object_name = file_name

        s3_client = boto3.client('s3', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)

        response = s3_client.upload_file(file_path,bucket,file_name)

    def generate_url(self,bucket,object_name):
        expiration = 3600
        s3_client = boto3.client('s3', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)

        object_url = s3_client.generate_presigned_url('get_object',Params={'Bucket':bucket,'Key':object_name},ExpiresIn=expiration)
        return object_url


class DalObject:
    def __init__(self,user,password):
        self.name = 'Dallen Grant'
        self.connection = None
        self.user = user
        self.password = password

    def set_testing(self, is_testing):
        if is_testing:
            self.__host = 'sequoia-staging.cfvzdoug1xvb.us-west-2.rds.amazonaws.com'
            self.__uid = self.user
            self.__pwd = self.password
            self.__port = 3306
            self.dbname = 'sequoia'
            self.driver = '{SQL server}'
        else:
            self.__host = 'sequoia-1.cfvzdoug1xvb.us-west-2.rds.amazonaws.com'
            self.__uid = self.user
            self.__pwd = self.password
            self.__port = 3306
            self.dbname = 'sequoia'
            self.driver = '{SQL server}'

    def ping_it(self, is_testing = True):
        self.set_testing(is_testing)
        if self.is_path_clear():
            try:
                self.connection = pymysql.connect(host=self.__host, user=self.__uid, port=self.__port, passwd=self.__pwd,
                                                  db=self.dbname)
                self.connection.close()
                return 'Ping'

            except RuntimeError:
                return 'The server did not respond.'


    def open_connection(self):
        if self.is_path_clear():
            self.connection = pymysql.connect(host=self.__host, user=self.__uid, port=self.__port, passwd=self.__pwd,
                                            db=self.dbname)

    def close_connection(self):
        self.connection.close()

    def test_connect_master(self):
        print(pandas.read_sql('show tables;', con=self.connection))

    # this may be very useful
    def test_to_sql(self, data, table):
        engine_string = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(self.__uid,self.__pwd,self.__host,self.__port,self.dbname)
        engine = create_engine(engine_string, echo=False)
        cnx = engine.raw_connection()
        # batch read
        # data = pd.read_sql('SELECT * FROM sample_table', cnx)
        # batch write
        data.to_sql(name=table, con=engine, if_exists='append', index=False)



    def id_cap(self,proc_name,proc_args):
        # this runs the capture step of each capture function
        cap_id = -1
        self.open_connection()
        obCursor = self.connection.cursor()
        obCursor.callproc(proc_name,args=proc_args)
        for result in obCursor.fetchall():
            cap_id = result[0]


        self.connection.commit()
        self.close_connection()
        return cap_id

    def is_path_clear(self):
        # this is ridiculous
        wait_counter = 0
        while threading.active_count() > 1:
            wait_counter +=1
            print('Waiting: '+str(wait_counter))
            time.sleep(1)

        return True

    def get_lookup(self,proc_name,column_names,filter_val=None):
        # returns the full results of a query
        result_set = []
        self.open_connection()
        cursor = self.connection.cursor()
        if filter_val:
            query_results = cursor.callproc(proc_name,args=(filter_val,))
        else:
            query_results = cursor.callproc(proc_name)

        for result in cursor.fetchall():
            result_set.append(result)

        result_df = pandas.DataFrame(data=result_set,columns = column_names,dtype=str)
        self.close_connection()
        return(result_df)


    def attribute_cap(self, newAttributeDesc, TableName):
        proc_name = 'sequoia.Attribute_capture_wrap'
        proc_args = (newAttributeDesc, TableName)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def country_cap(self,country,country_long_name,country_code,ecatcode):
        proc_name = 'sequoia.Country_capture_wrap'
        proc_args = (country,country_long_name,country_code,ecatcode)
        return_id = self.id_cap(proc_name,proc_args)
        return return_id

    def get_country_lookup(self):
        proc_name = 'sequoia.get_Country_lookup'
        column_names = ['CountryOfOriginId','CountryName','CountryLongName','CountryCode','ECATCountryCode']
        df_country_lookup = self.get_lookup(proc_name,column_names)
        return df_country_lookup


    def manufacturer_cap(self,ManufacturerName,SupplierName,FyManufacturerPrefix = -1):
        proc_name = 'sequoia.Manufacturer_capture_wrap'
        proc_args = (ManufacturerName, SupplierName, FyManufacturerPrefix)
        return_id = self.id_cap(proc_name, proc_args)

        return return_id

    def get_manufacturer_lookup(self):
        proc_name = 'sequoia.get_Manufacturer_lookup'
        column_names = ['ManufacturerId','ManufacturerName','SupplierName','FyManufacturerPrefix']
        df_manufacturer_lookup = self.get_lookup(proc_name,column_names)
        return df_manufacturer_lookup


    def vendor_cap(self, newVendorName, newVendorCode, newBrandName='', newMFCName='', newPrimaryGSAVendor=''):
        proc_name = 'sequoia.Vendor_capture_wrap'
        proc_args = (newVendorName, newBrandName, newVendorCode, newMFCName, newPrimaryGSAVendor)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_vendor_lookup(self):
        proc_name = 'sequoia.get_Vendor_lookup'
        column_names = ['VendorId','VendorName','VendorCode']
        df_vendor_lookup = self.get_lookup(proc_name,column_names)
        return df_vendor_lookup


    def category_cap(self,newCategoryName,newCategoryDesc):
        proc_name = 'sequoia.Category_capture_wrap'
        proc_args = (newCategoryName,newCategoryDesc)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def product_category_cap(self,newProductId,newCategoryId):
        proc_name = 'sequoia.ProductCategory_capture_wrap'
        proc_args = (newProductId,newCategoryId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_category_names(self):
        proc_name = 'sequoia.get_category_names'
        column_names = ['CategoryId','CategoryName','Category']
        df_category_lookup = self.get_lookup(proc_name,column_names)
        return df_category_lookup

    def get_word_category_associations(self):
        proc_name = 'gardener.get_word_cat_associations'
        column_names = ['word1','word2','category','is_good','count','percent_take']
        df_category_lookup = self.get_lookup(proc_name,column_names)
        df_category_lookup.loc[df_category_lookup['is_good'] == 0, 'is_good'] = -1
        return df_category_lookup

    def set_word_category_associations(self,newWord1,newWord2,newCategory,isGood,newCount):
        proc_name = 'gardener.set_word_cat_associations'
        proc_args = (newWord1,newWord2,newCategory,isGood,newCount)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def product_category_cap(self,newProductId,newParentCategoryId):
        proc_name = 'sequoia.ProductCategory_capture_wrap'
        proc_args = (newProductId,newParentCategoryId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def client_cap(self,newClientname,newClientdesc=''):
        proc_name = 'sequoia.Client_capture_wrap'
        proc_args = (newClientdesc,newClientname)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def client_special_price_cap(self,newClientSpecialPrice,newBeginDate,newEndDate,newBaseProductPriceId,newClientId,newClientSpecialPriceDesc=''):
        proc_name = 'sequoia.Client_capture_wrap'
        proc_args = (newClientSpecialPriceDesc,newClientSpecialPrice,newBeginDate,newEndDate,newBaseProductPriceId,newClientId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def component_set_name_cap(self,set_number=None,set_name=None):
        if (set_number and not set_name):
            set_name = ''
        if (set_name and not set_number):
            set_number = ''

        proc_name = 'sequoia.ComponentSetName_capture_wrap'
        proc_args = (set_name, set_number)
        return_id = self.id_cap(proc_name, proc_args)

        return return_id


    def component_set_cap(self,set_id=None,set_number=None,set_name=None, component_id=None, component_desc=None):
        # set can be id, Number, or Name
        # component can be id or name
        if (set_id):
            if (component_id):
                # by ID can attempt insert
                proc_name = 'sequoia.ComponentSet_capture_wrap'
                proc_args = (set_id, component_id)
            elif (component_desc):
                # get comp id
                component_id = self.attribute_cap(component_desc,'Component')
                # by ID can attempt insert
                proc_name = 'sequoia.ComponentSet_capture_wrap'
                proc_args = (set_id, component_id)

        elif (set_number or set_name):
            set_id = self.component_set_name_cap(set_number,set_name)
            if (component_id):
                # by ID can attempt insert
                proc_name = 'sequoia.ComponentSet_capture_wrap'
                proc_args = (set_id, component_id)
            elif (component_desc):
                component_id = self.attribute_cap(component_desc,'Component')
                # by ID can attempt insert
                proc_name = 'sequoia.ComponentSet_capture_wrap'
                proc_args = (set_id, component_id)


        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def lead_time_cap(self,newLeadTime,newExpeditedLeadTime):
        proc_name = 'sequoia.ExpectedLeadTime_capture_wrap'
        proc_args = (newLeadTime,newExpeditedLeadTime)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_lead_times(self):
        proc_name = 'sequoia.get_LeadTime_lookup'
        column_names = ['ExpectedLeadTimeId', 'LeadTimeDays', 'LeadTimeDaysExpedited']
        df_lead_time_lookup = self.get_lookup(proc_name, column_names)
        return df_lead_time_lookup

    def unspsc_code_cap(self,newUNSPSC,newUNSPSCTitle,newUNSPSCDesc):
        proc_name = 'sequoia.UNSPSC_capture_wrap'
        proc_args = (newUNSPSC,newUNSPSCTitle,newUNSPSCDesc)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_unspsc_id(self,newUNSPSC,newUNSPSCTitle=''):
        proc_name = 'sequoia.get_UNSPSC_id'
        proc_args = (newUNSPSC, newUNSPSCTitle)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def fsc_code_cap(self,newFSCCode, newFSCCodeName,newFSCCodeDesc):
        proc_name = 'sequoia.FSCCode_capture_wrap'
        proc_args = (newFSCCode, newFSCCodeName,newFSCCodeDesc)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_fsc_id(self, newFSCCode, newFSCCodeName=''):
        proc_name = 'sequoia.get_FSCCode_id'
        proc_args = (newFSCCode, newFSCCodeName)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def hazardous_code_cap(self,newHazardousCode, newHazardousCodeDesc, newHazardousCategory = ''):
        proc_name = 'sequoia.HazardousCode_capture_wrap'
        proc_args = (newHazardousCode, newHazardousCodeDesc, newHazardousCategory)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def naics_code_cap(self,newNAICSCode, newNAICSCodeDesc = ''):
        proc_name = 'sequoia.NAICSCode_capture_wrap'
        proc_args = (newNAICSCodeDesc,newNAICSCode)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def national_drug_code_cap(self,newNationalDrugCode,newNationalDrugCodeDesc=''):
        proc_name = 'sequoia.NationalDrugCode_capture_wrap'
        proc_args = (newNationalDrugCodeDesc,newNationalDrugCode)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def product_document_cap(self, newProductId, newProductCertificate='', newProductBrochure='',newProductSafetySheet='',newProductUrl=''):
        proc_name = 'sequoia.ProductDocument_capture_wrap'
        proc_args = (newProductCertificate, newProductBrochure,newProductSafetySheet,newProductUrl,newProductId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def product_seo_cap(self,newKeywords):
        proc_name = 'sequoia.ProductSEO_capture_wrap'
        # this stupid thing thinks a sting is a tuple
        # I mean it is, but that's not what we want
        # we want a single element tuple where the element is a string
        proc_args = (newKeywords,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def product_warranty_cap(self,newProductWarranty,newProductWarrantyType=''):
        proc_name = 'sequoia.ProductWarranty_capture_wrap'
        proc_args = (newProductWarranty,newProductWarrantyType)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def shipping_instructions_cap(self, newPackagingDesc, newshippingcode='', newIsFreeShipping=0, newIsColdChain=0):
        proc_name = 'sequoia.ShippingInstructions_capture_wrap'
        proc_args = (newshippingcode,newIsFreeShipping,newIsColdChain,newPackagingDesc)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def species_cap(self,newSpeciesscientificName,newSpeciesName=''):
        proc_name = 'sequoia.Species_capture_wrap'
        proc_args = (newSpeciesscientificName,newSpeciesName)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def tube_cap(self,newTubeCapacityDesc,newTubeSizeDesc,newTubeVolumeDesc):
        proc_name = 'sequoia.Tube_capture_wrap'
        proc_args = (newTubeCapacityDesc,newTubeSizeDesc,newTubeVolumeDesc)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def va_product_price_cap(self, lst_va_product_price):
        proc_name = 'sequoia.VAProductPrice_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_va_product_price)
        runner.start()

    def get_va_price_lookup(self):
        proc_name = 'sequoia.get_VAPrice_lookup'
        column_names = ['FyProductNumber','FyPartNumber','OnContract', 'VAApprovedListPrice',
                         'VAApprovedPercent', 'MfcDiscountPercent', 'VAContractModificationNumber','VAApprovedPriceDate','VAPricingApproved']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup


    def htme_product_price_cap(self,newIsVisible, newDateCatalogReceived, newHTMESellPrice, newHTMEApprovedPriceDate, newHTMEPricingApproved, newHTMEContractNumber, newHTMEContractModificationNumber, newHTMEProductGMPercent, newHTMEProductGMPrice):
        proc_name = 'sequoia.HTMEProductPrice_capture_wrap'
        proc_args = (newIsVisible, newDateCatalogReceived, newHTMESellPrice, newHTMEApprovedPriceDate, newHTMEPricingApproved, newHTMEContractNumber, newHTMEContractModificationNumber, newHTMEProductGMPercent, newHTMEProductGMPrice)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def gsa_product_price_cap(self,lst_gsa_product_price):
        proc_name = 'sequoia.GSAProductPrice_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_gsa_product_price)
        runner.start()

    def get_gsa_price_lookup(self):
        proc_name = 'sequoia.get_GSAPrice_lookup'
        column_names = ['FyProductNumber','FyPartNumber','OnContract', 'GSAApprovedListPrice',
                         'GSAApprovedPercent', 'MfcDiscountPercent', 'GSAContractModificationNumber','GSAApprovedPriceDate','GSAPricingApproved']
        df_gsa_price_lookup = self.get_lookup(proc_name,column_names)
        return df_gsa_price_lookup


    def ecat_product_price_cap(self,lst_ecat_product_price):
        proc_name = 'sequoia.ECATProductPrice_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_ecat_product_price)
        runner.start()

    def get_ecat_price_lookup(self):
        proc_name = 'sequoia.get_ECATPrice_lookup'
        column_names = ['FyProductNumber', 'FyPartNumber', 'OnContract', 'ECATApprovedListPrice',
                        'ECATApprovedPercent', 'MfcDiscountPercent', 'ECATContractModificationNumber',
                        'ECATApprovedPriceDate', 'ECATPricingApproved']
        df_ecat_price_lookup = self.get_lookup(proc_name, column_names)
        return df_ecat_price_lookup

    def fedmall_product_price_cap(self,newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate, newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice):
        proc_name = 'sequoia.FEDMALLProductPrice_capture_wrap'
        proc_args = (newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate, newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def oconus_product_cap(self,newShippingCostOconusECAT, newShippingCostDesc, newClientId, newProductPriceId):
        proc_name = 'sequoia.OconusProduct_capture_wrap'
        proc_args = (newShippingCostOconusECAT, newShippingCostDesc, newClientId, newProductPriceId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def product_promo_cap(self,newProductPromoSalePrice, newProductPromoBeginDate, newProductPromoEndDate, newFYPromoCost, newFYPromoDiscountPercent, newFYPromoPrice, newBaseProductPriceId):
        proc_name = 'sequoia.ProductPromo_capture_wrap'
        proc_args = (newProductPromoSalePrice, newProductPromoBeginDate, newProductPromoEndDate, newFYPromoCost, newFYPromoDiscountPercent, newFYPromoPrice, newBaseProductPriceId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def volume_unit_cap(self,newVolumeUnitsymbol, newVolumeUnitname):
        proc_name = 'sequoia.VolumeUnit_capture_wrap'
        proc_args = (newVolumeUnitsymbol, newVolumeUnitname)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def weight_unit_cap(self,newWeightUnitsymbol, newWeightUnitname):
        proc_name = 'sequoia.WeightUnit_capture_wrap'
        proc_args = (newWeightUnitsymbol, newWeightUnitname)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def length_unit_cap(self,newLengthUnitsymbol, newLengthUnitname):
        proc_name = 'sequoia.LengthUnit_capture_wrap'
        proc_args = (newLengthUnitsymbol, newLengthUnitname)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def image_size_cap(self,lst_product_image):
        proc_name = 'sequoia.ProductImageSize_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_image)
        runner.start()

    def get_image_names(self):
        proc_name = 'sequoia.get_ProductImageSize'
        column_names = ['ProductImageSizeId','ProductImageUrl','ProductImageName']
        df_image_name_lookup = self.get_lookup(proc_name,column_names)
        return df_image_name_lookup


    def image_cap(self, lst_product_image):
        proc_name = 'sequoia.ProductImage_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_image)
        runner.start()


    def unit_of_issue_symbol_cap(self,newUnitOfIssueSymbol, newUnitOfIssueSymbolName, ECATUnitOfIssueSymbol):
        proc_name = 'sequoia.UnitOfIssueSymbol_capture_wrap'
        proc_args = (newUnitOfIssueSymbol, newUnitOfIssueSymbolName, ECATUnitOfIssueSymbol)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def unit_of_issue_cap(self,newUnitOfIssueSymbolId,newunitcount,newUnitOfMeasureSymbolId):
        proc_name = 'sequoia.UnitOfIssue_capture_wrap'
        proc_args = (newunitcount,newUnitOfIssueSymbolId,newUnitOfMeasureSymbolId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def get_unit_of_issue_symbol_lookup(self):
        proc_name = 'sequoia.get_UnitOfIssueSymbol_lookup'
        column_names = ['UnitOfIssueSymbolId','UnitOfIssueSymbol']
        df_uoi_lookup = self.get_lookup(proc_name,column_names)
        return df_uoi_lookup


    def product_family_cap(self,newProductId,newFamilyId):
        proc_name = 'sequoia.ProductFamily_capture_wrap'
        proc_args = (newProductId,newFamilyId)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def family_cap(self,newFamilyDesc,newFamilyName):
        proc_name = 'sequoia.Family_capture_wrap'
        proc_args = (newFamilyDesc,newFamilyName)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def min_product_cap(self, lst_product_price):
        proc_name = 'sequoia.MinimumProduct_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_price)
        runner.start()


    def product_fill(self,lst_product_price):
        proc_name = 'sequoia.Product_fill'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_price)
        runner.start()


    def get_product_id_by_fy_catalog_number(self,newFYCatalogNumber):
        proc_name = 'sequoia.get_Product_id_by_fy_catalog_number'
        proc_args = (newFYCatalogNumber,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_product_id_by_manufacturer_part_number(self,newManufacturerPartNumber):
        proc_name = 'sequoia.get_Product_id_by_manufacturer_part_number'
        proc_args = (newManufacturerPartNumber,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_product_price_lookup(self):
        proc_name = 'sequoia.get_ProductPrice_lookup'
        column_names = ['ProductId','ProductPriceId','FyProductNumber']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup

    def get_product_fill_lookup(self):
        proc_name = 'sequoia.get_ProductFill_lookup'
        column_names = ['ProductId','ProductPriceId','FyProductNumber']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup

    def get_product_lookup(self):
        proc_name = 'sequoia.get_Product_lookup'
        column_names = ['ProductId','FyCatalogNumber','ManufacturerPartNumber']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup

    def min_product_price_cap(self,lst_product_price):
        proc_name = 'sequoia.MinimumProductPrice_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_price)
        runner.start()


    def product_price_fill(self, lst_product_price):
        proc_name = 'sequoia.ProductPrice_fill'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_product_price)
        runner.start()


    def max_product_price_cap(self,newFYCatalogNumber,newAmountPriceBreakLevel1,newAmountPriceBreakLevel2,newAmountPriceBreakLevel3,newQuantityPriceBreakLevel1,newQuantityPriceBreakLevel2,newQuantityPriceBreakLevel3,newVendorListPrice,newAllowPurchases,newFYPartNumber,newProductTaxClass,newVendorPartNumber,newProductId,newVendorId,newUnitOfIssueId,newUPC='',newVolume=-1,newWeight=-1,newSize='',newLength=-1,newVariantDesc='',newMinumumFlowTime='',newProfile='',newThicknessId=-1,newHeightId=-1,newDepthId=-1,newWidthId=-1,newCapacityId=-1,newTankCapacityId=-1,newVolumeUnitId=-1,newWeightUnitId=-1,newLengthUnitId=-1,newDimensionsId=-1,newInteriorDimensionsId=-1,newExteriorDimensionsId=-1,newMaterialId=-1,newColorId=-1,newSpeedId=-1,newTubeId=-1,newWeightRangeId=-1,newTemperatureRangeId=-1,newWavelengthId=-1,newWattageId=-1,newVoltageId=-1,newAmperageId=-1,newOuterDiameterId=-1,newInnerDiameterId=-1,newDiameterId=-1,newToleranceId=-1,newAccuracyId=-1,newMassId=-1,newApertureSizeId=-1,newApparelSizeId=-1,newParticleSizeId=-1,newPoreSizeId=-1):
        proc_name = 'sequoia.MinimumProductPrice_capture_wrap'
        proc_args = (newFYCatalogNumber,newAllowPurchases,newFYPartNumber,newProductTaxClass,newVendorPartNumber,newProductId,newVendorId,newUnitOfIssueId)
        return_product_price_id = self.id_cap(proc_name, proc_args)

        proc_name = 'sequoia.ProductPrice_fill_wrap'
        proc_args = (
        newProductPriceId, newUPC, newVolume, newWeight, newSize, newLength, newVariantDesc, newMinumumFlowTime,
        newProfile, newAmountPriceBreakLevel1,newAmountPriceBreakLevel2,newAmountPriceBreakLevel3,
        newQuantityPriceBreakLevel1, newQuantityPriceBreakLevel2, newQuantityPriceBreakLevel3,
        newThicknessId, newHeightId, newDepthId, newWidthId, newCapacityId, newTankCapacityId,
        newVolumeUnitId, newWeightUnitId, newLengthUnitId, newDimensionsId, newInteriorDimensionsId,
        newExteriorDimensionsId, newMaterialId, newColorId, newSpeedId, newTubeId, newWeightRangeId,
        newTemperatureRangeId, newWavelengthId, newWattageId, newVoltageId, newAmperageId, newOuterDiameterId,
        newInnerDiameterId, newDiameterId, newToleranceId, newAccuracyId, newMassId, newApertureSizeId,
        newApparelSizeId, newParticleSizeId, newPoreSizeId)
        success = self.id_cap(proc_name, proc_args)

        return success

    def base_price_cap(self, lst_base_product_price):
        proc_name = 'sequoia.BaseProductPrice_capture'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_base_product_price)
        runner.start()


    def get_product_price_id_by_fy_product_number(self,fy_product_number):
        proc_name = 'sequoia.get_ProductPrice_id_by_FYProductNumber'
        proc_args = (fy_product_number,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_product_price_id_by_fy_part_number(self,fy_part_number):
        proc_name = 'sequoia.get_ProductPrice_id_by_FYPartNumber'
        proc_args = (fy_part_number,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_product_price_id_by_vendor_part_number(self,vendor_part_number):
        proc_name = 'sequoia.get_ProductPrice_id_by_VendorPartNumber'
        proc_args = (vendor_part_number,)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id

    def get_base_product_price_lookup_by_vendor_id(self, vendor_id):
        proc_name = 'sequoia.get_BasePrice_lookup_vendor_id'
        column_names = ['FyProductNumber','ProductPriceId', 'FyCost']
        df_base_price_lookup = self.get_lookup(proc_name,column_names,vendor_id)
        return df_base_price_lookup

    def get_base_product_price_lookup(self):
        proc_name = 'sequoia.get_BasePrice_lookup'
        column_names = ['FyProductNumber','FyPartNumber','ProductPriceId','BaseProductPriceId']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup

    def set_bc_rtl(self, lst_bc_rtl):
        proc_name = 'sequoia.set_BigCommerceUpdateToggle'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, lst_bc_rtl)
        runner.start()

    def get_bc_rtl_state(self):
        proc_name = 'sequoia.get_BigCommerceUpdateToggle'
        column_names = ['ProductPriceId','FyProductNumber','BCPriceUpdateToggle','BCDataUpdateToggle']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup

    def naked_cap(self,thisdoesnothing,noreally):
        proc_name = 'sequoia.Unknown_capture_wrap'
        proc_args = (thisdoesnothing,noreally)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


class DataRunner(threading.Thread):
    def __init__(self, connection, proc_name, lst_data):
        threading.Thread.__init__(self)
        self.connection = connection
        self.proc_name = proc_name
        self.lst_data = lst_data

    def run(self):
        print('Runner report start: ' + self.proc_name)
        obCursor = self.connection.cursor()
        for each_ingest in self.lst_data:
            obCursor.callproc(self.proc_name, args=tuple(each_ingest))
        self.connection.commit()
        self.connection.close()
        print('Runner report end: ' + self.proc_name)




def test_local_connect():
    host = 'localhost'
    uid = 'root'
    pwd = 'Camembert20Brie'
    port = 3306
    dbname = 'sequoia'
    driver = '{SQL server}'

    connection = pymysql.connect(host=host, user=uid, port=port, passwd=pwd, db=dbname)

    print('connected')

    connection.close()



## end ##