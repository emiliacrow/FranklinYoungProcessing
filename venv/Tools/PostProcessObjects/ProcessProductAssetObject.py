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
    def __init__(self,df_product, user, password, is_testing, aws_access_key_id, aws_secret_access_key):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ProductAssets'
        self.obS3 = S3Object(aws_access_key_id, aws_secret_access_key)


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        self.vendor_name = self.vendor_name_selection()
        self.vendor_name = self.vendor_name.replace(',','')
        self.vendor_name = self.vendor_name.replace(' ','_')
        pass

    def define_new(self):
        self.current_assets = self.obDal.get_current_assets()
        match_headers = ['FyProductNumber','AssetType']
        self.current_assets['Filter'] = 'Pass'
        self.df_product = pandas.DataFrame.merge(self.df_product, self.current_assets,
                                                         how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Fail'), 'Filter'] = 'Update'

    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId','ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'IsDiscontinued','IsDiscontinued_x','IsDiscontinued_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        if 'Filter' in row:
            if row['Filter'] == 'Fail':
                self.obReporter.update_report('Fail', 'This product hasn\' been ingested.')
                return False
            elif 'CurrentAssetPath' in row:
                current_asset_path = row['CurrentAssetPath']
                proposed_asset_path = row['AssetPath']
                asset_type = row['AssetPath']
                if current_asset_path == proposed_asset_path and asset_type != 'Image':
                    self.obReporter.update_report('Fail', 'This product hasn\' been ingested.')
                    return False
                else:
                    self.obReporter.update_report('Alert', 'This product asset was overwritten.')


        else:
            self.obReporter.update_report('Fail', 'This product hasn\' been ingested.')
            return False


    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

        try:
            os.mkdir('temp_asset_files\\')
        except FileExistsError:
            pass

        for colName, row in df_line_product.iterrows():
            asset_type = row['AssetType']

            if asset_type == 'Brochure':
                print('do brochure processing')
                success, return_df_line_product = self.process_brochure(row, df_collect_product_base_data)

            elif asset_type == 'Certificate':
                print('do video processing')
                success, return_df_line_product = self.process_certificate(row, df_collect_product_base_data)

            elif asset_type == 'Image':
                # this will require some additional data like caption if any
                print('do image processing')
                success, return_df_line_product = self.process_image(row, df_collect_product_base_data)

            elif asset_type == 'SafetySheet':
                print('do safety sheet processing')
                success, return_df_line_product = self.process_safety_sheet(row, df_collect_product_base_data)

            elif asset_type == 'Video':
                print('do video processing')
                success, return_df_line_product = self.process_video(row, df_collect_product_base_data)


        shutil.rmtree(str(os.getcwd())+'temp_asset_files\\')

        return success, return_df_line_product

    def process_brochure(self, row, df_collect_product_base_data):
        success = True
        return success, df_collect_product_base_data

    def process_certificate(self, row, df_collect_product_base_data):
        # other_requirements = ['']
        success = True
        return success, df_collect_product_base_data

    def process_image(self, row, df_collect_product_base_data):
        # other_requirements = ['']
        success = True
        return success, df_collect_product_base_data


    def process_safety_sheet(self, row, df_collect_product_base_data):
        success = True
        bucket = 'franklin-young-safetysheet-bank'
        return_df_line_product = df_collect_product_base_data.copy()
        # step wise
        # pull values from df_ob
        # create temp path
        # iterate assets
        for colName, row in df_collect_product_base_data.iterrows():
            fy_product_number = row['FyProductNumber']
            asset_path = row['AssetPath']
            if 'http' in asset_path:
                # asset path is a url to fetch
                # this is the name of the file as pulled from the url
                url_name = asset_path.rpartition('/')[2]
                # this is the path which is placed, relatively to CWD
                temp_path = 'temp_asset_files\\'+url_name
                # This is the true path to the file
                whole_path = str(os.getcwd())+'\\'+temp_path

                if os.path.exists(whole_path):
                    object_name = whole_path.rpartition('\\')[2]
                else:
                    # Make http request for remote file data
                    asset_data = requests.get(asset_path)

                    if asset_data.ok:
                        # Save file data to local copy
                        with open(temp_path, 'wb')as file:
                            file.write(asset_data.content)
                    else:
                        self.obReporter.update_report('Alert','This url doesn\'t work.')
                        return False, return_df_line_product

            elif os.path.exists(asset_path):
                print('This is a file')
                object_name = asset_path.rpartition('\\\\')[2]
                whole_path = asset_path
            else:
                return False, return_df_line_product

            # this sets the actual url to our file, see this example
            # https://franklin-young-image-bank.s3.us-west-2.amazonaws.com/CONSOLIDATED+STERILIZER+SYSTEMS/PT-SR-24AB-26AB-ADVPRO.jpg
            safety_sheet_url = 'https://'+bucket+'.s3.us-west-2.amazonaws.com/'+self.vendor_name+'/'+object_name

            # This is the name to put in our bucket
            # THOMAS/imagename.png
            s3_name = self.vendor_name + '/' + object_name

            # this puts the object into s3
            self.obS3.put_file(whole_path, s3_name, bucket)

            product_id = row['ProductId']

            # this sets the data in the database
            self.obIngester.set_productsafetysheet_cap(self.is_last, product_id, safety_sheet_url, object_name)
            return True, return_df_line_product


    def process_video(self, row, return_df_line_product):
        success = True
        return success, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_productbrochure_cleanup()
        self.obIngester.set_productcertificate_cleanup()
        self.obIngester.set_productimage_cleanup()
        self.obIngester.set_productsafetysheet_cap()
        self.obIngester.set_productvideo_cleanup()


## end ##