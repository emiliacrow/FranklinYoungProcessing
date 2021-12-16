# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import os
import shutil
import pandas
import requests

from Tools.FY_DAL import S3Object
from Tools.BasicProcess import BasicProcessObject


class ProcessProductAssetObject(BasicProcessObject):
    req_fields = ['FyProductNumber','AssetPath', 'AssetType']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ProductAssets'
        self.obS3 = S3Object(self.aws_access_key_id, self.aws_secret_access_key)


    def batch_preprocessing(self):
        self.define_new()
        pass

    def define_new(self):
        self.current_assets = self.obDal.get_current_assets()
        match_headers = ['FyProductNumber']
        self.current_assets['Filter'] = 'Pass'
        self.df_product = pandas.DataFrame.merge(self.df_product, self.current_assets,
                                                         how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Fail'), 'Filter'] = 'Update'


    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Fail':
                self.obReporter.update_report('Fail', 'This product hasn\' been ingested.')
                return False
        else:
            self.obReporter.update_report('Fail', 'This product hasn\' been ingested.')
            return False


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        os.mkdir('temp_asset_files\\')
        for colName, row in df_line_product.iterrows():
            asset_type = row['AssetType']

            if asset_type == 'Image':
                # this will require some additional data like caption if any
                print('do image processing')
                return self.process_image(row, df_collect_product_base_data)

            elif asset_type == 'SafetySheet':
                print('do safety sheet processing')
                return self.process_safety_sheet(row, df_collect_product_base_data)

            elif asset_type == 'Brochure':
                print('do brochure processing')
                return self.process_brochure(row, df_collect_product_base_data)

            elif asset_type == 'Video':
                print('do video processing')
                return self.process_video(row, df_collect_product_base_data)

            elif asset_type == 'Certificate':
                print('do video processing')
                return self.process_certificate(row, df_collect_product_base_data)

        shutil.rmtree('temp_asset_files\\')

        return success, return_df_line_product

    def process_image(self, row, return_df_line_product):
        # other_requirements = ['']
        success = True
        return success, return_df_line_product

    def process_safety_sheet(self, row, return_df_line_product):
        success = True
        bucket = 'franklin-young-safetysheet-bank'

        # step wise
        # pull values from df_ob
        # create temp path
        # iterate assets
        for colName, row in df_collect_product_base_data.iterrows():
            fy_product_number = row['FyProductNumber']
            print('Product number:', fy_product_number)
            asset_path = row['AssetPath']
            print('Asset path:', asset_path)
            if 'http' in asset_path:
                print('This is a url')
                url_name = asset_path.rpartition('\\')[2]
                print('Url name', url_name)

                temp_path = 'temp_asset_files\\'+url_name
                print('temp path', temp_path)
                whole_path = str(os.getcwd())+temp_path
                print('Whole path', whole_path)


                # Make http request for remote file data
                asset_data = requests.get(asset_path)
                # Save file data to local copy
                with open(temp_path, 'wb')as file:
                    file.write(asset_data.content)


            else:
                print('This is a file')
                object_name = asset_path.rpartition('\\\\')[2]

            # https://franklin-young-image-bank.s3.us-west-2.amazonaws.com/CONSOLIDATED+STERILIZER+SYSTEMS/PT-SR-24AB-26AB-ADVPRO.jpg
            safety_sheet_url = 'https://'+bucket+'.s3.us-west-2.amazonaws.com/'+vendor_name+'/'+object_name
            # this is the put object step
            # THOMAS/imagename.png
            s3_name = vendor_name + '/' + object_name

            print('put object:', whole_path, s3_name, bucket)
            #self.obS3.put_file(asset_path, s3_name, bucket)

            print('Push to db:', product_id, safety_sheet_url, safety_sheet_name)
            #self.obIngester.set_productsafetysheet_cap(self.is_last, product_id, safety_sheet_url, safety_sheet_name)


        return success, return_df_line_product

    def process_brochure(self, row, return_df_line_product):
        success = True
        return success, return_df_line_product

    def process_video(self, row, return_df_line_product):
        success = True
        return success, return_df_line_product


    def process_changes(self, df_collect_product_base_data):
        for colName, row in df_collect_product_base_data.iterrows():
            price_toggle = 0
            data_toggle = 0

            product_price_id = row['ProductPriceId']
            fy_product_number = row['FyProductNumber']

            if 'BCPriceUpdateToggle' in row:
                price_toggle = row['BCPriceUpdateToggle']
            if 'BCDataUpdateToggle' in row:
                data_toggle = row['BCDataUpdateToggle']

        self.obIngester.set_bigcommerce_rtl(self.is_last, product_price_id, fy_product_number, price_toggle, data_toggle)
        return True, df_collect_product_base_data