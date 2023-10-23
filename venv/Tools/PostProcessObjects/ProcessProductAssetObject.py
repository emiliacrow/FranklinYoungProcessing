# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import os
import shutil
import pandas
import requests

from PIL import Image
from PIL import UnidentifiedImageError

from Tools.FY_DAL import S3Object
from Tools.BasicProcess import BasicProcessObject


class ProcessProductAssetObject(BasicProcessObject):
    req_fields = ['FyCatalogNumber','FyProductNumber','ManufacturerName', 'ManufacturerPartNumber','VendorName','VendorPartNumber','AssetPath', 'AssetType']

    sup_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, aws_access_key_id, aws_secret_access_key):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'ProductAssets'
        self.obS3 = S3Object(aws_access_key_id, aws_secret_access_key)


    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new(is_asset=True)
        self.lst_image_objects = self.obS3.get_object_list('franklin-young-image-bank')
        self.lst_document_objects = self.obS3.get_object_list('franklin-young-document-bank')


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
        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] in ['Partial', 'Base Pricing']:
            self.obReporter.update_report('Alert', 'Passed filtering as partial product')
            return False

        elif row['Filter'] == 'Ready':
            self.obReporter.update_report('Alert', 'Passed filtering as updatable')
            return True

        elif row['Filter'] == 'Possible Duplicate':
            self.obReporter.update_report('Alert', 'Review product numbers for possible duplicates')
            return False

        else:
            self.obReporter.update_report('Fail', 'Failed filtering')
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

            # document is the catch all.
            if asset_type in ['Document','SafetyDataSheet','Certificate','Product_Brochure','Warranty','Image']:
                success, df_collect_product_base_data = self.process_document(row, df_collect_product_base_data, asset_type)
            elif asset_type == 'BCImages':
                # depricated
                success, df_collect_product_base_data = self.process_bc_images(row, df_collect_product_base_data)
            elif asset_type == 'Video':
                success, df_collect_product_base_data = self.process_video(row, df_collect_product_base_data)
            else:
                self.obReporter.update_report('Fail','Asset type not recognized')
                success = False

        # the idea being to use a temporary location and remove them all after
        # shutil.rmtree(str(os.getcwd())+'temp_asset_files\\')

        return success, df_collect_product_base_data


    def process_bc_images(self, row, df_collect_product_base_data):
        ## DEPRICATED ##
        df_return_product = df_collect_product_base_data.copy()
        # this may also need to include functionality for changing the ACL for an asset.
        # which mean we might want to pull the asset list from S3 after all

        success = True
        bucket = 'franklin-young-image-bank'
        # step wise
        # pull values from df_ob
        # create temp path
        # iterate assets
        asset_path = row['AssetPath']
        manufacturer_name = row['ManufacturerName']

        manufacturer_name = manufacturer_name.replace(',', '')
        manufacturer_name = manufacturer_name.replace(' ', '_')

        images_from_file = []

        # depricated
        if '|' in asset_path:
            lst_asset_path = asset_path.split('|')
            for each_asset_path in lst_asset_path:
                asset_file_name = each_asset_path.partition('Product Image File: ihwx.')[2].partition(',')[0]
                asset_file_name = self.obValidator.bc_image_name(asset_file_name)

                temp_path = 'temp_asset_files\\'+asset_file_name
                # This is the true path to the file
                whole_path = str(os.getcwd())+'\\'+temp_path

                asset_url = each_asset_path.partition('Product Image URL: ')[2].partition(',')[0]

                image_caption = each_asset_path.partition('Product Image Description: ')[2].partition(',')[0]

                if os.path.exists(whole_path):
                    object_name = whole_path.rpartition('\\')[2]
                else:

                    # Make http request for remote file data
                    asset_data = requests.get(asset_url)

                    if asset_data.ok:
                        # Save file data to local copy
                        with open(temp_path, 'wb')as file:
                            file.write(asset_data.content)
                        object_name = whole_path.rpartition('\\')[2]
                    else:
                        continue


                images_from_file.append([whole_path,object_name,image_caption])


        else:
            asset_file_name = asset_path.partition('Product Image File: ihwx.')[2].partition(',')[0]
            asset_file_name = self.obValidator.bc_image_name(asset_file_name)

            temp_path = 'temp_asset_files\\' + asset_file_name
            # This is the true path to the file
            whole_path = str(os.getcwd()) + '\\' + temp_path

            asset_url = asset_path.partition('Product Image URL: ')[2].partition(',')[0]

            image_caption = asset_path.partition('Product Image Description: ')[2].partition(',')[0]

            if os.path.exists(whole_path):
                object_name = whole_path.rpartition('\\')[2]
                self.obReporter.update_report('Alert', 'This was previously scraped')
            else:
                # Make http request for remote file data
                asset_data = requests.get(asset_url)

                if asset_data.ok:
                    # Save file data to local copy
                    with open(temp_path, 'wb')as file:
                        file.write(asset_data.content)
                    object_name = whole_path.rpartition('\\')[2]
                    df_return_product['AssetObjectName'] = [object_name]
                    self.obReporter.update_report('Alert', 'This asset was scraped')
                else:
                    self.obReporter.update_report('Fail', 'This url doesn\'t work.')
                    return False, df_return_product

            images_from_file.append([whole_path,object_name,image_caption])


        if len(images_from_file) <= 0:
            return False, df_return_product

        product_id = row['ProductId']
        asset_type = row['AssetType']

        for each_image_set in images_from_file:
            whole_path = each_image_set[0]
            object_name = each_image_set[1]
            if object_name == '':
                continue
            image_caption = each_image_set[2]
            s3_name = manufacturer_name + '/' + object_name

            # this puts the object into s3
            # only send to s3 if it doesn't already exist
            if s3_name not in self.lst_image_objects:
                self.obS3.put_file(whole_path, s3_name, bucket)
                self.lst_image_objects.append(s3_name)

            image_width, image_height = self.get_image_size(whole_path)

            self.obIngester.set_productimage(product_id, s3_name, object_name, 0, image_caption, image_width, image_height)

        return True, df_return_product


    def get_image_size(self, image_path):
        try:
            current_image = Image.open(image_path)
            image_width, image_height = current_image.size
        except UnidentifiedImageError:
            image_width, image_height = 0, 0

        return image_width, image_height


    def process_document(self, row, df_collect_product_base_data, asset_type):
        df_return_product = df_collect_product_base_data.copy()
        # this may also need to include functionality for changing the ACL for an asset.
        # which mean we might want to pull the asset list from S3 after all

        success = True
        if asset_type != 'Image':
            bucket = 'franklin-young-document-bank'
        else:
            bucket = 'franklin-young-image-bank'
        # step wise
        # pull values from df_ob
        # create temp path
        # iterate assets
        asset_path = row['AssetPath']
        manufacturer_name = row['ManufacturerName']

        if 'http' in asset_path:
            # asset path is a url to fetch
            # this is the name of the file as pulled from the url
            url_name = asset_path.rpartition('/')[2]
            # this is the path which is placed, relatively to CWD
            temp_path = 'temp_asset_files\\'+url_name
            # This is the true path to the file
            whole_path = str(os.getcwd())+'\\'+temp_path
            df_return_product['WholeFilePath'] = [whole_path]

            if os.path.exists(whole_path):
                object_name = whole_path.rpartition('\\')[2]
                self.obReporter.update_report('Alert','This was previously scraped')
            else:
                # Make http request for remote file data
                asset_data = requests.get(asset_path)

                if asset_data.ok:
                    # Save file data to local copy
                    with open(temp_path, 'wb')as file:
                        file.write(asset_data.content)
                    object_name = whole_path.rpartition('\\')[2]
                    df_return_product['AssetObjectName'] = [object_name]
                    self.obReporter.update_report('Alert','This asset was scraped')
                else:
                    self.obReporter.update_report('Fail','This url doesn\'t work.')
                    return False, df_return_product

        elif os.path.exists(asset_path):
            object_name = asset_path.rpartition('\\')[2]
            whole_path = asset_path
            df_return_product['AssetObjectName'] = [object_name]
        else:
            self.obReporter.update_report('Alert','Please check that the path is a url or file path')
            return False, df_return_product


        if 'FranklinYoungDefaultImage' in whole_path:
            s3_name = 'FranklinYoungDefaultImage/' + object_name
        else:
            s3_name = manufacturer_name + '/' + object_name

        # we check if the documents
        if 'CurrentAssetPath' in row:
            current_asset_path = row['CurrentAssetPath']
            if current_asset_path == whole_path and asset_type != 'Image' and asset_type != 'Video':
                self.obReporter.update_report('Fail', 'This asset already exists.')
                return False, df_return_product
            else:
                # other documents are over written
                self.obReporter.update_report('Alert', 'This product asset was overwritten.')


        # this puts the object into s3
        # only send to s3 if it doesn't already exist
        if asset_type == 'Image':
            if s3_name not in self.lst_image_objects:
                self.obS3.put_file(whole_path, s3_name, bucket)
                self.lst_image_objects.append(s3_name)

        elif s3_name not in self.lst_document_objects:
            self.obS3.put_file(whole_path, s3_name, bucket)
            self.lst_document_objects.append(s3_name)

        # images are linked to the product
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
            self.obIngester.set_productdocument_cap(product_id, s3_name, object_name, asset_type, document_preference)

        else:
            caption = ''
            if 'ImageCaption' in row:
                caption = row['ImageCaption']
            image_width, image_height = self.get_image_size(whole_path)

            self.obIngester.set_productimage(product_id, manufacturer_name, s3_name, object_name, document_preference, caption, image_width, image_height)


        return True, df_return_product


    def process_video(self, row, return_df_line_product):
        # we just store the reference
        product_id = row['ProductId']
        video_path = row['AssetPath']
        video_caption = ''
        video_preference = 0

        if 'VideoCaption' in row:
            video_caption = row['VideoCaption']

        if 'AssetPreference' in row:
            video_preference = row['AssetPreference']


        self.obIngester.set_productvideo_cap(product_id, video_path, video_caption,
                                                video_preference)
        return True, return_df_line_product


    def trigger_ingest_cleanup(self):
        self.obIngester.set_productdocument_cleanup()
        self.obIngester.set_productimage_cleanup()
        self.obIngester.set_productvideo_cleanup()


def test_frame():
    print('I don\'t just do things')
    print('You have to write some test code first.')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    test_frame()



## end ##