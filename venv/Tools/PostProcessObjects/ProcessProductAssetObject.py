# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import pandas

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
        pass

    def process_product_line(self, df_line_product):
        success = True
        df_collect_product_base_data = df_line_product.copy()

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
        # if url,
        for colName, row in df_collect_product_base_data.iterrows():
            fy_product_number = row['FyProductNumber']
            asset_path = row['AssetPath']
            if 'http' in asset_path:
                print('This is a url')
            else:
                print('This is a file')
                object_name = asset_path.rpartition('\\\\')[2]

            safety_sheet_url = path_in_s3
            # this is the put object step
            # THOMAS/imagename.png
            s3_name = vendor_name + '/' + object_name

            self.obS3.put_image(image_path, s3_name, bucket)

        self.obIngester.set_productsafetysheet_cap(self.is_last, product_id, safety_sheet_url, safety_sheet_name)
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