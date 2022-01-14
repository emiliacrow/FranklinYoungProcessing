# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import os
import shutil
import pandas
import requests

from PIL import Image

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


    def define_new(self):
        self.current_assets = self.obDal.get_current_assets()
        match_headers = ['FyProductNumber']
        self.current_assets['Filter'] = 'Pass'
        self.df_product = self.df_product.merge(self.current_assets, how='left', on=match_headers)
        self.df_product.loc[(self.df_product['Filter'] != 'Pass'), 'Filter'] = 'Update'

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
                    return True

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
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

            asset_type = row['AssetType']

            if asset_type in ['Document','SafetyDataSheet','Certificate','Brochure','Warranty','Image']:
                success, df_collect_product_base_data = self.process_document(row, df_collect_product_base_data, asset_type)
            elif asset_type == 'Video':
                success, df_collect_product_base_data = self.process_video(row, df_collect_product_base_data)
            else:
                self.obReporter.update_report('Fail','Asset type not recognized')
                success = False

        # the idea being to use a temporary location and remove them all after
        # shutil.rmtree(str(os.getcwd())+'temp_asset_files\\')

        return success, df_collect_product_base_data


    def process_document(self, row, df_collect_product_base_data, asset_type):
        success = True
        if asset_type != 'Image':
            bucket = 'franklin-young-document-bank'
        else:
            bucket = 'franklin-young-image-bank'
        # step wise
        # pull values from df_ob
        # create temp path
        # iterate assets
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
                    object_name = whole_path.rpartition('\\')[2]
                else:
                    self.obReporter.update_report('Alert','This url doesn\'t work.')
                    return False, df_collect_product_base_data

        elif os.path.exists(asset_path):
            print('This is a file')
            object_name = asset_path.rpartition('\\\\')[2]
            whole_path = asset_path
        else:
            return False, df_collect_product_base_data

        # this sets the actual url to our file, see this example
        # https://franklin-young-image-bank.s3.us-west-2.amazonaws.com/CONSOLIDATED+STERILIZER+SYSTEMS/PT-SR-24AB-26AB-ADVPRO.jpg
        safety_sheet_url = 'https://'+bucket+'.s3.us-west-2.amazonaws.com/'+self.vendor_name+'/'+object_name

        # This is the name to put in our bucket
        # THOMAS/imagename.png
        s3_name = self.vendor_name + '/' + object_name

        # this puts the object into s3
        self.obS3.put_file(whole_path, s3_name, bucket)

        product_id = row['ProductId']
        asset_type = row['AssetType']

        success, document_preference = self.row_check(row,'AssetPreference')
        if success:
            success, document_preference = self.float_check(document_preference, 'AssetPreference')
            if success:
                document_preference = int(document_preference)
            else:
                document_preference = 0
                self.obReporter.update_report('Alert','AssetPreference must be a number')
        else:
            document_preference = 0
            self.obReporter.update_report('Alert','AssetPreference set to 0')


        if asset_type != 'Image':
            self.obIngester.set_productdocument_cap(product_id, safety_sheet_url, object_name, asset_type,
                                                    document_preference)

        else:
            caption = ''
            if 'ImageCaption' in row:
                caption = row['ImageCaption']
            image_width, image_height = self.get_image_size(whole_path)
            self.obIngester.set_productimage(product_id, safety_sheet_url, object_name, document_preference, caption, image_width, image_height)


        return True, df_collect_product_base_data


    def get_image_size(self, image_path):
        current_image = Image.open(image_path)
        image_width, image_height = current_image.size
        return image_width, image_height


    def process_video(self, row, return_df_line_product):
        product_id = row['ProductId']
        video_path = row['AssetPath']
        video_caption = ''
        video_preference = 0

        if 'VideoCaption' in row:
            video_caption = row['VideoCaption']

        if 'VideoPreference' in row:
            video_preference = row['VideoPreference']

        self.obIngester.set_productvideo_cap(product_id, video_path, video_caption,
                                                video_preference)
        return True, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_productdocument_cleanup()
        self.obIngester.set_productimage_cleanup()
        self.obIngester.set_productvideo_cleanup()


## end ##