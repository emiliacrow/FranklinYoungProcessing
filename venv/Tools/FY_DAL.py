# CreatedBy: Emilia Crow
# CreateDate: 20210330
# Updated: 20220318
# CreateFor: Franklin Young International

import os
import sys
import time
import boto3
import pandas
import pymysql
import threading
import subprocess


from pymysql.err import OperationalError
from sqlalchemy import create_engine

class S3Object:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.name = 'Simple Stanley'
        self.region_name = 'us-west-2'
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def put_file(self, file_path, file_name, bucket, object_name = None):
        if object_name is None:
            object_name = file_name


        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
        s3_client = boto3.client('s3', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)

        extra_args = {'ACL': 'public-read'}
        response = s3_client.upload_file(file_path, bucket, file_name, ExtraArgs = extra_args)


    def get_object_list(self, bucket):
        s3_client = boto3.client('s3', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                   aws_secret_access_key=self.aws_secret_access_key)
        s3_objects = s3_client.list_objects(Bucket=bucket)
        lst_objects = []
        banked_objects = s3_objects['Contents']
        for each_banked_object in banked_objects:
            doc_name = each_banked_object['Key']
            lst_objects.append(doc_name)

        return lst_objects


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

    def get_lookup(self, proc_name, column_names,filter_val=None):
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

        if 'ManufacturerPartNumber' in result_df.columns:
            result_df['ManufacturerPartNumber'] = result_df['ManufacturerPartNumber'].str.upper()

        if 'VendorPartNumber' in result_df.columns:
            result_df['VendorPartNumber'] = result_df['VendorPartNumber'].str.upper()

        return(result_df)


    def attribute_cap(self, newAttributeDesc, TableName):
        proc_name = 'sequoia.Attribute_capture_wrap'
        proc_args = (newAttributeDesc, TableName)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def country_cap(self,country,country_long_name,country_code,ecatcode,is_taa_compliant=0):
        proc_name = 'sequoia.Country_capture_wrap'
        proc_args = (country,country_long_name,country_code,ecatcode,is_taa_compliant)
        return_id = self.id_cap(proc_name,proc_args)
        return return_id

    def get_country_lookup(self):
        proc_name = 'sequoia.get_Country_lookup'
        column_names = ['CountryOfOriginId','CountryName','CountryLongName','CountryCode','ECATCountryCode','IsTAACompliant']
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


    def vendor_cap(self, newVendorCode, newVendorName, newBrandName=''):
        proc_name = 'sequoia.Vendor_capture_wrap'
        proc_args = (newBrandName, newVendorCode, newVendorName)
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

    def get_product_category(self):
        proc_name = 'sequoia.get_product_category'
        column_names = ['FyCatalogNumber','Category','CategoryId']
        df_category_lookup = self.get_lookup(proc_name,column_names)
        return df_category_lookup


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

    def set_word_category_associations(self,newWord1,newWord2,newCategory,isGood = 1,newCount = 1):
        proc_name = 'gardener.set_word_cat_associations'
        proc_args = (newWord1.lower(),newWord2.lower(),newCategory,isGood,newCount)
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
        column_names = ['ExpectedLeadTimeId', 'LeadTime', 'LeadTimeExpedited']
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


    def ecat_product_price_cap(self,lst_ecat_product_price):
        proc_name = 'sequoia.ECATProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`ECATProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_ecat_product_price)
        runner.start()

    def get_ecat_price_lookup(self):
        proc_name = 'sequoia.get_ECATPrice_lookup'
        column_names = ['ManufacturerName','ManufacturerPartNumber','FyCatalogNumber', 'FyProductNumber', 'VendorName',
                        'VendorPartNumber', 'db_FyCost']
        df_ecat_price_lookup = self.get_lookup(proc_name, column_names)
        return df_ecat_price_lookup


    def fedmall_product_price_cap(self,newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate, newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice):
        proc_name = 'sequoia.FEDMALLProductPrice_capture_wrap'
        proc_args = (newIsVisible, newDateCatalogReceived, newFEDMALLSellPrice, newFEDMALLApprovedPriceDate, newFEDMALLPricingApproved, newFEDMALLContractNumber, newFEDMALLContractModificationNumber, newFEDMALLProductGMPercent, newFEDMALLProductGMPrice)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


    def gsa_product_price_cap(self,lst_gsa_product_price):
        proc_name = 'sequoia.GSAProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`GSAProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_gsa_product_price)
        runner.start()

    def get_gsa_price_lookup(self):
        proc_name = 'sequoia.get_GSAPrice_lookup'
        column_names = ['FyProductNumber','VendorPartNumber','GSAOnContract','db_ContractedManufacturerPartNumber', 'GSAApprovedListPrice',
                         'GSAApprovedPercent', 'MfcDiscountPercent', 'GSAContractModificationNumber','GSAApprovedPriceDate','GSAPricingApproved']
        df_gsa_price_lookup = self.get_lookup(proc_name,column_names)
        return df_gsa_price_lookup


    def htme_product_price_cap(self,lst_htme_product_price):
        proc_name = 'sequoia.HTMEProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`HTMEProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_htme_product_price)
        runner.start()

    def get_htme_price_lookup(self):
        proc_name = 'sequoia.get_HTMEPrice_lookup'
        column_names = ['FyProductNumber', 'VendorPartNumber', 'HTMEOnContract', 'ECATApprovedListPrice', 'db_ContractedManufacturerPartNumber',
                        'HTMEContractModificationNumber', 'HTMEApprovedPriceDate', 'HTMEPricingApproved']
        df_htme_price_lookup = self.get_lookup(proc_name, column_names)
        return df_htme_price_lookup


    def va_product_price_cap(self, lst_va_product_price):
        proc_name = 'sequoia.VAProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`VAProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_va_product_price)
        runner.start()

    def get_va_price_lookup(self):
        proc_name = 'sequoia.get_VAPrice_lookup'
        column_names = ['FyProductNumber','VendorPartNumber','VAOnContract','db_ContractedManufacturerPartNumber', 'VAApprovedListPrice',
                         'VAApprovedPercent', 'MfcDiscountPercent', 'VAContractModificationNumber','VAApprovedPriceDate','VAPricingApproved']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup


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


    def product_image_capture(self,lst_product_image):
        proc_name = 'sequoia.ProductImage_capture'
        proc_statement = 'CALL `sequoia`.`ProductImage_capture`(%s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_image)
        runner.start()

    def get_image_names(self):
        proc_name = 'sequoia.get_ProductImageSize'
        column_names = ['ProductImageSizeId','ProductImageUrl','ProductImageName']
        df_image_name_lookup = self.get_lookup(proc_name,column_names)
        return df_image_name_lookup


    def image_cap(self, lst_product_image):
        proc_name = 'sequoia.ProductImage_capture'
        proc_statement = 'CALL `sequoia`.`ProductImage_capture`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_image)
        runner.start()


    def unit_of_issue_symbol_cap(self,newUnitOfIssueSymbol, newUnitOfIssueSymbolName, ECATUnitOfIssueSymbol = ''):
        proc_name = 'sequoia.UnitOfIssueSymbol_capture_wrap'
        proc_args = (newUnitOfIssueSymbol, newUnitOfIssueSymbolName, ECATUnitOfIssueSymbol)
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
        proc_statement = 'CALL `sequoia`.`MinimumProduct_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_price)
        runner.start()


    def product_fill(self,lst_product_price):
        proc_name = 'sequoia.Product_fill'
        proc_statement = 'CALL `sequoia`.`Product_fill`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_price)
        runner.start()


    def get_product_action_review_lookup(self):
        proc_name = 'sequoia.get_ProductActionReview2_lookup'
        column_names = ['ProductId', 'ManufacturerName', 'ManufacturerPartNumber', 'FyCatalogNumber', 'ProductPriceId','FyProductNumber','VendorName','VendorPartNumber','BaseProductPriceId','db_IsDiscontinued']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup

    def get_product_fill_lookup(self):
        proc_name = 'sequoia.get_ProductFill_lookup'
        column_names = ['ProductId','ProductPriceId','FyProductNumber']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup


    def get_discon_products(self):
        proc_name = 'sequoia.get_discon_products_lookup'
        column_names = ['ProductPriceId','FyProductNumber','VendorName','VendorPartNumber','IsDiscontinued']
        df_product_lookup = self.get_lookup(proc_name,column_names)
        return df_product_lookup

    def set_discon_product_price(self, lst_product_price):
        proc_name = 'sequoia.set_discon_product'
        proc_statement = 'CALL `sequoia`.`set_discon_product`(%s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_price)
        runner.start()


    def min_product_price_cap(self,lst_product_price):
        proc_name = 'sequoia.MinimumProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`MinimumProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_price)
        runner.start()


    def product_price_fill(self, lst_product_price):
        proc_name = 'sequoia.ProductPrice_fill'
        proc_statement = 'CALL `sequoia`.`ProductPrice_fill`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                         '%s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_product_price)
        runner.start()


    def base_price_cap(self, lst_base_product_price):
        proc_name = 'sequoia.BaseProductPrice_capture'
        proc_statement = 'CALL `sequoia`.`BaseProductPrice_capture`(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_base_product_price)
        runner.start()


    def get_base_product_price_lookup(self):
        proc_name = 'sequoia.get_BasePrice_lookup'
        column_names = ['ProductId', 'ManufacturerPartNumber', 'FyCatalogNumber', 'ProductPriceId', 'FyProductNumber',
                        'VendorPartNumber', 'BaseProductPriceId', 'db_Discount', 'db_MarkUp_sell', 'db_MarkUp_list']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup


    def get_category_match_desc(self, new_description):
        proc_name = 'gardener.get_category_match_description'
        column_names = ['CategoryName', 'CategoryId', 'CategoryDesc','VoteCount']
        proc_args = (new_description,)
        df_category_match = self.get_lookup(proc_name, column_names, proc_args)
        return df_category_match


    def get_overrides(self):
        proc_name = 'sequoia.get_override_lookup'
        column_names = ['ManufacturerPartNumber', 'db_IsProductNumberOverride']
        df_base_price_lookup = self.get_lookup(proc_name,column_names)
        return df_base_price_lookup


    def productdocument_cap(self, lst_productdocuments):
        proc_name = 'sequoia.ProductDocument_capture'
        proc_statement = 'CALL `sequoia`.`ProductDocument_capture`(%s, %s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_productdocuments)
        runner.start()


    def productvideo_cap(self, lst_productvideo):
        proc_name = 'sequoia.ProductVideo_capture'
        proc_statement = 'CALL `sequoia`.`ProductVideo_capture`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_productvideo)
        runner.start()


    def get_current_assets(self):
        proc_name = 'sequoia.get_current_assets'
        column_names = ['FyProductNumber','ProductId','AssetPath','CurrentAssetType']
        df_current_assets = self.get_lookup(proc_name,column_names)
        return df_current_assets


    def get_toggles(self):
        proc_name = 'sequoia.get_Toggles'
        column_names = ['ProductId','ProductPriceId','BaseProductPriceId','ECATProductPriceId','HTMEProductPriceId',
                        'GSAProductPriceId','VAProductPriceId','FyProductNumber','VendorPartNumber']
        df_toggles = self.get_lookup(proc_name,column_names)
        return df_toggles


    def set_bc_toggles(self, lst_bc_toggles):
        proc_name = 'sequoia.set_BC_toggles'
        proc_statement = 'CALL `sequoia`.`set_BC_toggles`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_bc_toggles)
        runner.start()


    def set_discon(self, lst_discon_toggles):
        proc_name = 'sequoia.set_is_discon_allow_purchase'
        proc_statement = 'CALL `sequoia`.`set_is_discon_allow_purchase`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_discon_toggles)
        runner.start()

    def set_is_visible(self, lst_is_vis_toggles):
        proc_name = 'sequoia.set_is_visible'
        proc_statement = 'CALL `sequoia`.`set_is_visible`(%s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_is_vis_toggles)
        runner.start()

    def set_update_asset(self, lst_update_assets_toggles):
        proc_name = 'sequoia.set_update_assets'
        proc_statement = 'CALL `sequoia`.`set_update_assets`(%s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_update_assets_toggles)
        runner.start()

    def set_ecat_toggles(self, lst_ecat_toggles):
        proc_name = 'sequoia.set_ecat_toggles'
        proc_statement = 'CALL `sequoia`.`set_ecat_toggles`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_ecat_toggles)
        runner.start()

    def set_htme_toggles(self, lst_htme_toggles):
        proc_name = 'sequoia.set_htme_toggles'
        proc_statement = 'CALL `sequoia`.`set_htme_toggles`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_htme_toggles)
        runner.start()

    def set_gsa_toggles(self, lst_gsa_toggles):
        proc_name = 'sequoia.set_gsa_toggles'
        proc_statement = 'CALL `sequoia`.`set_gsa_toggles`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_gsa_toggles)
        runner.start()

    def set_va_toggles(self, lst_va_toggles):
        proc_name = 'sequoia.set_va_toggles'
        proc_statement = 'CALL `sequoia`.`set_va_toggles`(%s, %s, %s, %s);'
        self.open_connection()
        runner = DataRunner(self.connection, proc_name, proc_statement, lst_va_toggles)
        runner.start()



    def naked_cap(self,thisdoesnothing,noreally):
        proc_name = 'sequoia.Unknown_capture_wrap'
        proc_args = (thisdoesnothing,noreally)
        return_id = self.id_cap(proc_name, proc_args)
        return return_id


class DataRunner(threading.Thread):
    def __init__(self, connection, proc_name, proc_statement, lst_data):
        threading.Thread.__init__(self)
        self.connection = connection
        self.proc_name = proc_name
        self.proc_statement = proc_statement
        self.lst_data = lst_data

    def run(self):
        print('Runner report start: ' + self.proc_name)
        obCursor = self.connection.cursor()
        fail_retries = []

        count = 0
        for each_item in self.lst_data:
            count += 1
            print('Runner count: {0}'.format(count))
            # this value here for testing
            #obCursor.callproc(self.proc_name, args = each_item)
            try:
                obCursor.callproc(self.proc_name, args = each_item)
            except OperationalError:
                fail_retries.append(each_item)
                print('Wait fail count: {0}'.format(len(fail_retries)))

        # this is for executing many in the DB which can be faster
        # obCursor.executemany(self.proc_statement, self.lst_data)

        drops = 0
        count = 0
        print('Retry count: {0}'.format(len(fail_retries)))
        for each_item in fail_retries:
            count += 1
            print('Runner count: {0}'.format(count))
            try:
                obCursor.callproc(self.proc_name, args = each_item)
            except OperationalError:
                drops += 1
                print('Wait fail count: {0}'.format(drops))


        self.connection.commit()
        self.connection.close()

        print('Runner report end: ' + self.proc_name)


def get_lookup(conx, proc_name, column_names,filter_val=None):
    # returns the full results of a query
    result_set = []
    cursor = conx.cursor()
    if filter_val:
        query_results = cursor.callproc(proc_name,args=(filter_val,))

    else:
        query_results = cursor.callproc(proc_name)

    for result in cursor.fetchall():
        result_set.append(result)

    result_df = pandas.DataFrame(data=result_set,columns = column_names,dtype=str)

    conx.close()

    return(result_df)


def test_local_connect():
    tell_all = 'Look, man, whatever.'
    print(tell_all)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test_local_connect()


## end ##