# CreatedBy: Emilia Crow
# CreateDate: 20210526
# Updated: 20210813
# CreateFor: Franklin Young International

import os
import shutil
import pandas
import numpy as np

from PIL import Image

from Tools.FY_DAL import S3Object
from Tools.FY_DAL import DalObject
from Tools.ToolBox import FileFinder
from Tools.Ingestion import IngestionObject

# gui objects
from Tools.ProgressBar import YesNoDialog
from Tools.ProgressBar import TextBoxObject
from Tools.ProgressBar import ProgressBarWindow
from Tools.ProgressBar import JoinSelectionDialog

# other processing objects
from Tools.FileProcessObject import FileProcessor
from Tools.BaseDataLoaderObject import BaseDataLoader
from Tools.ProductAgniKaiObject import ProductAgniKaiObject
from Tools.CategoryProcessingObject import CategoryProcessor
from Tools.PostProcessObjects.BigCommerceRTLObject import BigCommerceRTLObject as BC_RTL_Object
from Tools.PostProcessObjects.DiscontinuationObject import DiscontinueObject
from Tools.PostProcessObjects.ProcessProductAssetObject import ProcessProductAssetObject

# product objects
from Tools.ProductObjects.MinimumProductObject import MinimumProduct
from Tools.ProductObjects.MinimumProductObject import UpdateMinimumProduct

from Tools.ProductObjects.FillProductObject import FillProduct
from Tools.ProductObjects.FillProductObject import UpdateFillProduct

from Tools.ProductObjects.MinimumPriceObject import MinimumProductPrice
from Tools.ProductObjects.MinimumPriceObject import UpdateMinimumProductPrice

from Tools.ProductObjects.FillPriceObject import FillProductPrice
from Tools.ProductObjects.FillPriceObject import UpdateFillProductPrice

# pricing objects
from Tools.PriceObjects.BasePriceObject import BasePrice
from Tools.PriceObjects.BasePriceObject import UpdateBasePrice

from Tools.PriceObjects.VAPriceObject import VAPrice
from Tools.PriceObjects.GSAPriceObject import GSAPrice
from Tools.PriceObjects.ECATPriceObject import ECATPrice
from Tools.PriceObjects.HTMEPriceObject import HTMEPrice
from Tools.PriceObjects.FEDMALLPriceObject import FEDMALLPrice


class Pathways():
    def __init__(self):
        self.name = 'The Way'
        self.split_chunk_size = 10000
        self.full_file_count = 0
        self.np_nan = np.nan
        self.obFileFinder = FileFinder()

        self.perm_file = os.getcwd() + '\\venv\Assets\SequoiaCredentials.txt'
        self.user, self.password, self.aws_access_key_id, self.aws_secret_access_key, self.start_path = self.test_perm_file()


    def test_perm_file(self):
        self.start_path = ''
        try:
            with open(self.perm_file) as f:
                lines = f.readlines()

        except FileNotFoundError:
            file_ident_success, message_or_path = self.obFileFinder.ident_file('Please find your Sequoia permissions file.')
            if file_ident_success:
                shutil.copy(message_or_path, self.perm_file)

            with open(self.perm_file) as f:
                lines = f.readlines()

        f.close()

        for line in lines:
            if 'Username:' in line:
                self.user = line.replace('Username:','')
                self.user = self.user.replace('\n','')

            if 'Password:' in line:
                self.password = line.replace('Password:','')
                self.password = self.password.replace('\n','')

            if 'aws_access_key_id:' in line:
                self.aws_access_key_id = line.replace('aws_access_key_id:', '')
                self.aws_access_key_id = self.aws_access_key_id.replace('\n', '')

            if 'aws_secret_access_key:' in line:
                self.aws_secret_access_key = line.replace('aws_secret_access_key:','')
                self.aws_secret_access_key = self.aws_secret_access_key.replace('\n','')

            if 'file_path_return:' in line:
                self.start_path = line.replace('file_path_return:','')
                self.start_path = self.start_path.replace('\n','')

        return self.user, self.password,self.aws_access_key_id,self.aws_secret_access_key, self.start_path


    def set_perm_file(self):
        file_text = "\n".join(['Username:'+self.user, 'Password:'+self.password,'aws_access_key_id:'+self.aws_access_key_id
                                  ,'aws_secret_access_key:'+self.aws_secret_access_key, 'file_path_return:'+self.start_path])
        with open(self.perm_file, 'w') as f:
            f.write(file_text)


    def file_processing_pathway(self, is_testing, file_action_selected):
        self.success = False
        self.message = 'File processing pathway'
        # this doesn't behave like the rest of them
        if file_action_selected == 'File Appender Tool':
            self.success, self.message = self.file_appender_tool()
        elif file_action_selected == 'File Merger Tool':
            self.success, self.message = self.file_merger_tool()

        elif file_action_selected == 'File Splitter Tool':
            self.success, self.message = self.file_splitter_tool()
        else:
            file_ident_success, message_or_path = self.obFileFinder.ident_file('Select file to process: '+file_action_selected, path = self.start_path)
            if file_ident_success == False:
                return file_ident_success, message_or_path

            self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\','\\')
            self.set_perm_file()

            if 'Category' in file_action_selected:
                self.df_product = self.obFileFinder.read_xlsx()
                self.obCategoryProcessor = CategoryProcessor(self.df_product, self.user, self.password, is_testing,
                                                             file_action_selected)
                self.success, self.message = self.obCategoryProcessor.begin_process()
                self.df_product = self.obCategoryProcessor.get_df()

                self.obFileFinder.write_xlsx(self.df_product,
                                             'cat_' + file_action_selected.replace('Category ', ''))

            elif file_action_selected == 'Product Action Review':
                self.df_product = self.obFileFinder.read_xlsx()

                self.obAgniKai = ProductAgniKaiObject(self.df_product, self.user, self.password, is_testing, file_action_selected)

                self.success, self.message = self.obAgniKai.begin_process()
                self.df_product = self.obAgniKai.get_df()
                # perform split and create files
                if 'Filter' in self.df_product.columns:
                    self.message = self.perform_file_split(['Filter'])
                else:
                    self.obFileFinder.write_xlsx(self.df_product,'split_fail')

            else:
                self.df_product = self.obFileFinder.read_xlsx()
                self.obFileProcessor = FileProcessor(self.df_product, self.user, self.password, is_testing, file_action_selected, self.aws_access_key_id, self.aws_secret_access_key)
                self.success, self.message = self.obFileProcessor.begin_process()
                self.df_product = self.obFileProcessor.get_df()

                self.obFileFinder.write_xlsx(self.df_product,'file_process')


        return self.success, self.message


    def file_splitter_tool(self):
        self.success = True
        self.message = 'It\'s finished'

        # which file you wanna split
        file_ident_success, message_or_path = self.obFileFinder.ident_file('Select file to split.', path = self.start_path)
        if file_ident_success == False:
            return file_ident_success, message_or_path

        self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
        self.set_perm_file()

        self.df_product = self.obFileFinder.read_xlsx()

        # get the column on which to split
        column_headers = list(self.df_product.columns)
        self.onMergeDialog = JoinSelectionDialog(column_headers, 'Please select one column:',icon='split')
        self.onMergeDialog.exec()
        # split on column or column
        split_on = self.onMergeDialog.get_selected_items()

        self.message = self.perform_file_split(split_on, b_sub_dir=True)

        return True, self.message


    def perform_file_split(self, split_on, b_sub_dir = False):
        self.full_file_count = 0

        df_column_alone = self.df_product[split_on]

        df_column_alone = df_column_alone.drop_duplicates(subset=split_on)

        split_values = []
        for each_row in df_column_alone.values:
            split_values.append(each_row[0])

        # split the data based on the values in the column
        for each_value in split_values:
            # break layer
            file_name = str(each_value).replace(' ', '_')
            file_name = file_name.replace('.', '_')
            file_name = file_name.replace(',', '_')
            file_name = file_name.replace('\\', '')
            file_name = file_name.replace('/', '')
            file_name = file_name.replace('__', '_')

            df_layer = self.df_product.loc[(self.df_product[split_on[0]] == each_value)]

            df_layer = df_layer.replace(r'^\s*$', self.np_nan, regex=True)
            # this removes all columns with all nan
            df_layer = df_layer.dropna(axis=1, how='all')

            # get the size
            chunk_size = len(df_layer.index)
            # if the size is bigger than the limit
            if chunk_size > self.split_chunk_size:
                chunk_count = chunk_size / self.split_chunk_size
                # we determine the number of chunks to make
                if chunk_count % 1 > 0:
                    chunk_count = int(chunk_count / 1) + 1

                file_number = 1
                chunk_layer = 0
                while chunk_count != 0:
                    # get each layer
                    df_layer_chunk = df_layer.iloc[chunk_layer:chunk_layer + self.split_chunk_size, :]
                    chunk_layer += self.split_chunk_size
                    layer_name = file_name + '_' + str(file_number)

                    self.full_file_count += 1
                    pb_name = 'Writing file #{0}: {1}'.format(str(self.full_file_count),layer_name)
                    self.obFileFinder.write_xlsx_sub_folder(df_layer_chunk, layer_name, False, pb_name)

                    file_number += 1
                    chunk_count -= 1
            else:
                pb_name = 'Writing file #{0}: {1}'.format(str(self.full_file_count),file_name)
                if b_sub_dir:
                    layer_name = file_name + '_0'
                    self.obFileFinder.write_xlsx_sub_folder(df_layer, layer_name, False, pb_name)
                else:
                    self.obFileFinder.write_xlsx(df_layer, file_name, False, pb_name)

                self.full_file_count += 1

        self.message = 'Created {} files.'.format(self.full_file_count)
        return self.message


    def file_appender_tool(self):
        self.full_file_count = 1
        file_ident_success, message_or_path = self.obFileFinder.ident_files('Select files to append.', path = self.start_path)

        if file_ident_success:
            first_file = message_or_path.pop()

            self.df_second_product = self.obFileFinder.read_xlsx(first_file)

            for each_file in message_or_path:
                self.full_file_count += 1
                self.df_intermediate_product = self.obFileFinder.read_xlsx(each_file)

                self.obProgressBar = ProgressBarWindow('Appending data...', icon='append-a')
                self.obProgressBar.set_anew(10)
                self.obProgressBar.show()
                self.obProgressBar.update_unknown()

                self.df_second_product = self.df_second_product.append(self.df_intermediate_product)

                self.obProgressBar.close()

            self.obFileFinder.write_xlsx(self.df_second_product, '(appended-results)', False)
            self.message = '{} files have been combined.'.format(self.full_file_count)

            self.start_path = (first_file.rpartition('\\')[0]).replace('\\\\', '\\')
            self.set_perm_file()
            return file_ident_success, self.message
        else:
            return file_ident_success, message_or_path


    def file_merger_tool(self):
        self.success = True
        self.message = 'It\'s finished'

        file_ident_success, message_or_path = self.obFileFinder.ident_file('Select first File: This one is merged into the second file.')
        if file_ident_success == False:
            return file_ident_success, message_or_path
        else:
            self.df_first_product = self.obFileFinder.read_xlsx()

            self.obYNBox = YesNoDialog('Append a file?')

            self.obYNBox.initUI('Append file dialog.','Add more data to the first dataframe?',icon='append-a')
            while self.obYNBox.yes_selected == True:
                file_ident_success, message_or_path = self.obFileFinder.ident_file('Select intermediate File: This one is appended to the first file.')
                if file_ident_success:
                    self.df_intermediate_product = self.obFileFinder.read_xlsx()

                    self.obProgressBar = ProgressBarWindow('Appending data...',icon='append-a')
                    self.obProgressBar.set_anew(10)
                    self.obProgressBar.show()
                    self.obProgressBar.update_unknown()

                    self.df_first_product = self.df_first_product.append(self.df_intermediate_product)

                    self.obProgressBar.close()
                else:
                    break
                self.obYNBox.initUI('Append file dialog.','Add more data to the first dataframe?',icon='append-a')

            self.obYNBox.initUI('Merge dialog.', 'Merge with another file?', icon='merge')

            if self.obYNBox.yes_selected:
                file_ident_success, message_or_path = self.obFileFinder.ident_file('Select second File: This file recieves the first file.')
                if file_ident_success:

                    self.df_second_product = self.obFileFinder.read_xlsx()
                    self.obYNBox.initUI('Append file dialog.','Add more data to the second dataframe?',icon='append-b')
                    while self.obYNBox.yes_selected == True:
                        file_ident_success, message_or_path = self.obFileFinder.ident_file('Select intermediate File: This one is appended to the second file.')
                        if file_ident_success:
                            self.df_intermediate_product = self.obFileFinder.read_xlsx()

                            self.obProgressBar = ProgressBarWindow('Appending data...', icon='append-b')
                            self.obProgressBar.set_anew(10)
                            self.obProgressBar.show()
                            self.obProgressBar.update_unknown()

                            self.df_second_product = self.df_second_product.append(self.df_intermediate_product)

                            self.obProgressBar.close()
                        else:
                            break

                        self.obYNBox.initUI('Append file dialog.','Add more data to the second dataframe?',icon='append-b')

                    first_headers = set(self.df_first_product.columns)
                    second_headers = set(self.df_second_product.columns)

                    header_overlap = list(first_headers.intersection(second_headers))
                    if header_overlap:

                        self.onMergeDialog = JoinSelectionDialog(header_overlap, instruction='Please select the merge columns:',icon='merge')
                        self.onMergeDialog.exec()

                        join_list = self.onMergeDialog.get_selected_items()

                        if len(join_list)==0:
                            self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
                            self.set_perm_file()
                            return False,'No matching columns assigned.'

                        elif len(join_list) > 1:
                            join_phrase = 'Join on: {0} and {1} more.'.format(join_list[0],str(len(join_list)-1))
                        else:
                            join_phrase = 'Join on: {0}.'.format(join_list[0])
                        self.obYNBox.initUI(join_phrase,'Confirm?')

                        if self.obYNBox.yes_selected:
                            self.obProgressBar = ProgressBarWindow('Merging data...',icon='merge')
                            self.obProgressBar.set_anew(10)
                            self.obProgressBar.show()
                            self.obProgressBar.update_unknown()

                            self.df_combined_product = pandas.DataFrame.merge(self.df_second_product, self.df_first_product,
                                                                          how='outer', on=join_list)

                            self.obProgressBar.close()

                            self.obFileFinder.write_xlsx(self.df_combined_product, '(match-result)')

                            self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
                            self.set_perm_file()
                            return True, 'Output has been generated'
                    else:
                        self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
                        self.set_perm_file()
                        return False, 'No column header overlap detected'

                else:
                    return file_ident_success, message_or_path

            else:
                self.obYNBox.initUI('Output dialog.', 'Output the first file without merge?', icon='append-a')
                if self.obYNBox.yes_selected:
                    self.obFileFinder.write_xlsx(self.df_first_product, '(appended-results)')

                    self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
                    self.set_perm_file()
                    return True, 'Output has been generated.'
                else:
                    self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\', '\\')
                    self.set_perm_file()
                    return False, 'Canceled by user at output.'

            self.obYNBox.close()

        return self.success, self.message


    def image_load_tool(self, is_testing):
        # set objects
        self.obDal = DalObject(self.user, self.password)
        self.obS3 = S3Object(self.aws_access_key_id, self.aws_secret_access_key)
        self.bucket = 'franklin-young-image-bank'
        self.obIngester = IngestionObject(self.obDal)
        self.obDal.set_testing(is_testing)
        self.region_name = 'us-west-2'

        # ident path
        file_success, clean_image_paths = self.obFileFinder.ident_directory('Select image directory to process:')

        if not file_success:
            return False, 'No files selected'

        self.start_path = (clean_image_paths[0][0].rpartition('\\')[0]).replace('\\\\', '\\')
        self.set_perm_file()

        # get vendor name from path
        folder_name = (clean_image_paths[0][0].rpartition('\\\\')[0]).rpartition('\\\\')[2]

        # get list of image names
        current_image_names = self.obDal.get_image_names()
        new_images = []

        for each_image in clean_image_paths:
            image_name = each_image[1]

            # check it against db
            if image_name not in current_image_names['ProductImageName'].values:
                new_images.append(each_image)

        self.obProgressBarWindow = ProgressBarWindow('Loading Image Files',icon='progress')
        self.obProgressBarWindow.show()
        self.obProgressBarWindow.set_anew(len(new_images))
        self.obProgressBarWindow.update_unknown()
        # set progress bar
        is_last = False
        p_bar = 0

        for each_image in new_images:
            # get image size
            image_path = each_image[0]
            image_name = each_image[1]

            current_image = Image.open(image_path)

            image_width, image_height = current_image.size

            s3_name = folder_name + '/' + image_name

            self.obS3.put_file(image_path, s3_name, self.bucket)

            # this provides a link for downloading the image
            # image_presigned_url = self.obS3.generate_url(self.bucket, s3_name)

            p_bar += 1
            if p_bar == len(new_images):
                is_last = True
            self.obIngester.image_size_cap(s3_name, image_name, image_width, image_height)

            self.obProgressBarWindow.update_bar(p_bar)

        self.obProgressBarWindow.close()

        success = True
        message = 'Loaded {} new images'.format(p_bar)

        return success, message


    def base_data_pathway(self, is_testing, table_to_load):
        self.success = False
        self.message = 'Base data pathway'
        self.success, self.message = self.base_data_manual_load(is_testing, table_to_load)
        return self.success, self.message

    def base_data_manual_load(self, is_testing, table_to_load):
        # set the dal
        self.obDal = DalObject(self.user, self.password)
        self.obDal.set_testing(is_testing)
        self.obIngester = IngestionObject(self.obDal)

        if table_to_load == 'Category':
            cat_id = self.obIngester.manual_ingest_category()
            if (cat_id == 0):
                return True, 'Canceled by user action'
            elif (cat_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Category with id: {0}'.format(cat_id)

        elif table_to_load == 'Country':
            coo_id = self.obIngester.manual_ingest_country()
            if (coo_id == 0):
                return True, 'Canceled by user action'
            elif (coo_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Country of Origin with id: {0}'.format(coo_id)

        elif table_to_load == 'FSC Codes':
            fsc_id = self.obIngester.manual_ingest_fsc_code()
            if (fsc_id == 0):
                return True, 'Canceled by user action'
            elif (fsc_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new FSC Code with id: {0}'.format(fsc_id)

        elif table_to_load == 'Hazardous Code':
            haz_id = self.obIngester.manual_ingest_hazard_code()
            if (haz_id == 0):
                return True, 'Canceled by user action'
            elif (haz_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Hazard Code with id: {0}'.format(haz_id)

        elif table_to_load == 'Manufacturer':
            df_manufacturers = self.obDal.get_manufacturer_lookup()

            manufacturer_name_list = df_manufacturers["ManufacturerName"].tolist()
            manufacturer_name_list = list(dict.fromkeys(manufacturer_name_list))

            man_id = self.obIngester.manual_ingest_manufacturer(lst_manufacturer_names=manufacturer_name_list)
            if (man_id == 0):
                return True, 'Canceled by user action'
            elif (man_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Manufacturer with id: {0}'.format(man_id)

        elif table_to_load == 'NAICS Code':
            naics_id = self.obIngester.manual_ingest_naics_code()
            if (naics_id == 0):
                return True, 'Canceled by user action'
            elif (naics_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new NAICS Code with id: {0}'.format(naics_id)

        elif table_to_load == 'Unit of Issue-Symbol':
            uois_id = self.obIngester.manual_ingest_uois_code()
            if (uois_id == 0):
                return True, 'Canceled by user action'
            elif (uois_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Unit of Issue Symbol with id: {0}'.format(uois_id)

        elif table_to_load == 'UNSPSC Codes':
            unspsc_id = self.obIngester.manual_ingest_unspsc_code()
            if (unspsc_id == 0):
                return True, 'Canceled by user action'
            elif (unspsc_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new UNSPSC with id: {0}'.format(unspsc_id)

        elif table_to_load == 'Vendor':
            df_vendors = self.obDal.get_vendor_lookup()

            vendor_name_list = df_vendors["VendorName"].tolist()
            vendor_name_list = list(dict.fromkeys(vendor_name_list))

            ven_id = self.obIngester.manual_ingest_vendor(lst_vendor_names=vendor_name_list)
            if (ven_id == 0):
                return True, 'Canceled by user action'
            elif (ven_id == -1):
                return False, 'You failed to fill a required field'
            else:
                return True, 'Loaded a new Vendor with id: {0}'.format(ven_id)

        else:
            return False, 'No file available.'

    def ingest_data_pathway(self, is_testing, ingestion_action_selected):
        self.success, message_or_path = self.obFileFinder.ident_file('Select product data file: '+ingestion_action_selected, path = self.start_path)
        if self.success == False:
            return self.success, message_or_path

        self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\','\\')
        self.set_perm_file()


        self.success = False
        b_inter_files = True
        self.message = 'Ingest data pathway'

        # I have an alternate idea here where we could assign a write counter
        # the write counter would start with the number of steps and sub 1 each time it runs one
        # when it hits 0 it writes like magic!
        #self.obYNBox = YesNoDialog('Write intermediate files?')
        #self.obYNBox.initUI('Intermediate file dialog.', 'Would you like to write intermediate files?')
        #if self.obYNBox.yes_selected == True:
        #    b_inter_files = True

        self.df_product = self.obFileFinder.read_xlsx()
        all_steps = ['1-Minimum Product Ingestion(3 steps)','2-Full Product Ingestion(5 steps)','3-Fill Product Attributes(2 steps)','4-Minimum Product Price(3 steps)','5-Base Pricing(1 step)','GSA Pricing','VA Pricing']

        self.obYNBox.close()

        if ingestion_action_selected in ['1-Minimum Product Ingestion(3 steps)','2-Full Product Ingestion(5 steps)']:
            self.obMinProduct = MinimumProduct(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obMinProduct.begin_process()
            self.df_product = self.obMinProduct.get_df()
            if b_inter_files:
                self.obFileFinder.write_xlsx(self.df_product,'MinProd')
            if self.success == False:
                return self.success, self.message

        if ingestion_action_selected in ['1-Minimum Product Ingestion(3 steps)','2-Full Product Ingestion(5 steps)','4-Minimum Product Price(3 steps)']:
            self.obMinPrice = MinimumProductPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obMinPrice.begin_process()
            self.df_product = self.obMinPrice.get_df()
            if b_inter_files or ingestion_action_selected == '2-Minimum Product Ingestion(2 steps)':
                self.obFileFinder.write_xlsx(self.df_product,'MinPrice')

            if self.success == False or ingestion_action_selected == '2-Minimum Product Ingestion(2 steps)':
                return self.success, self.message

        if ingestion_action_selected in ['2-Full Product Ingestion(5 steps)','3-Fill Product Attributes(2 steps)',]:
            self.obFillProduct = FillProduct(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obFillProduct.begin_process()
            self.df_product = self.obFillProduct.get_df()
            if b_inter_files:
                self.obFileFinder.write_xlsx(self.df_product,'FillProd')
            if self.success == False:
                return self.success, self.message

        if ingestion_action_selected in ['2-Full Product Ingestion(5 steps)','3-Fill Product Attributes(2 steps)',]:
            self.obFillPrice = FillProductPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obFillPrice.begin_process()
            self.df_product = self.obFillPrice.get_df()
            if b_inter_files or ingestion_action_selected == '3-Fill Product(2 steps)':
                self.obFileFinder.write_xlsx(self.df_product,'FillPrice')
            if self.success == False or ingestion_action_selected == '3-Fill Product(2 steps)':
                return self.success, self.message

        if ingestion_action_selected in ['1-Minimum Product Ingestion(3 steps)','2-Full Product Ingestion(5 steps)','4-Minimum Product Price(3 steps)','5-Base Pricing(1 step)']:
            self.obBasePrice = BasePrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obBasePrice.begin_process()
            self.df_product = self.obBasePrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'BasePrice')
            if self.success == False or ingestion_action_selected == '1-Minimum Product Ingestion(3 steps)' or ingestion_action_selected == '2-Full Product Ingestion(5 steps)' or ingestion_action_selected == '4-Minimum Product Price(3 steps)' or ingestion_action_selected == '5-Base Pricing(1 step)':
                return self.success, self.message

        if ingestion_action_selected == 'VA Pricing':
            self.obVAPrice = VAPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obVAPrice.begin_process()
            self.df_product = self.obVAPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'VAPrice')
            return self.success, self.message

        if ingestion_action_selected == 'GSA Pricing':
            self.obGSAPrice = GSAPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obGSAPrice.begin_process()
            self.df_product = self.obGSAPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'GSAPrice')
            return self.success, self.message

        if ingestion_action_selected == 'HTME Pricing':
            self.obHTMEPrice = HTMEPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obHTMEPrice.begin_process()
            self.df_product = self.obHTMEPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'HTMEPrice')
            return self.success, self.message

        if ingestion_action_selected == 'ECAT Pricing':
            self.obECATPrice = ECATPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obECATPrice.begin_process()
            self.df_product = self.obECATPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'ECATPrice')
            return self.success, self.message

        if ingestion_action_selected == 'FEDMALL Pricing':
            self.obFEDMALLPrice = FEDMALLPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obFEDMALLPrice.begin_process()
            self.df_product = self.obFEDMALLPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'FEDMALLPrice')
            return self.success, self.message

        return False, 'Process not built.'


    def update_data_pathway(self, is_testing, update_action_selected):
        self.success, message_or_path = self.obFileFinder.ident_file('Select product data file: '+update_action_selected, path = self.start_path)
        if self.success == False:
            return self.success, message_or_path

        self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\','\\')
        self.set_perm_file()

        self.full_process = True
        self.success = False
        b_inter_files = False
        self.message = 'Update data pathway'

        self.obYNBox = YesNoDialog('Write intermediate files?')
        self.obYNBox.initUI('Intermediate file dialog.', 'Would you like to write intermediate files?')
        if self.obYNBox.yes_selected == True:
            b_inter_files = True

        self.df_product = self.obFileFinder.read_xlsx()
        all_steps = ['1-Update Minimum Product Data(3 steps)',
                     '1.5-Update Minimum Product Price Data(2 steps)',
                     '2-Update Full Product(5 steps)',
                     '3-Update Product Attributes(2 steps)',
                     '4-Update Base Pricing(1 step)',
                     'Update GSA Pricing',
                     'Update VA Pricing',
                     'Update HTME Pricing',
                     'Update ECAT Pricing',
                     'Update FEDMALL Pricing']

        self.obYNBox.close()

        if update_action_selected in ['1-Update Minimum Product Data(3 steps)', '2-Update Full Product(5 steps)']:

            self.obYNBox = YesNoDialog('Process new products?')
            self.obYNBox.initUI('Include products dialog.', 'Include new products in processing?')
            if self.obYNBox.yes_selected == True:
                self.full_process = True
            else:
                self.full_process = False

            self.obUpdateMinProduct = UpdateMinimumProduct(self.df_product, self.user, self.password, is_testing,full_process=self.full_process)

            self.success, self.message = self.obUpdateMinProduct.begin_process()
            self.df_product = self.obUpdateMinProduct.get_df()
            if b_inter_files:
                self.obFileFinder.write_xlsx(self.df_product, 'UpdateMinProd')
            if self.success == False:
                return self.success, self.message

        if update_action_selected in ['1-Update Minimum Product Data(3 steps)', '1.5-Update Minimum Product Price Data(2 steps)',  '2-Update Full Product(5 steps)']:
            self.obUpdateMinPrice = UpdateMinimumProductPrice(self.df_product, self.user, self.password, is_testing,full_process=self.full_process)
            self.success, self.message = self.obUpdateMinPrice.begin_process()
            self.df_product = self.obUpdateMinPrice.get_df()
            if b_inter_files:
                self.obFileFinder.write_xlsx(self.df_product, 'UpdateMinPrice')
            if self.success == False:
                return self.success, self.message

        if update_action_selected in ['2-Update Full Product(5 steps)', '3-Update Product Attributes(2 steps)']:
            self.UpdateFillProduct = UpdateFillProduct(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.UpdateFillProduct.begin_process()
            self.df_product = self.UpdateFillProduct.get_df()
            if b_inter_files:
                self.obFileFinder.write_xlsx(self.df_product, 'UpdateFillProduct')
            if self.success == False:
                return self.success, self.message

        if update_action_selected in ['2-Update Full Product(5 steps)', '3-Update Product Attributes(2 steps)']:
            self.UpdateFillPrice = UpdateFillProductPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.UpdateFillPrice.begin_process()
            self.df_product = self.UpdateFillPrice.get_df()
            if b_inter_files or ingestion_action_selected == '3-Update Product Attributes(2 steps)':
                self.obFileFinder.write_xlsx(self.df_product, 'UpdateFillProductPrice')
            if self.success == False or update_action_selected == '3-Update Product Attributes(2 steps)':
                return self.success, self.message

        if update_action_selected in ['1-Update Minimum Product Data(3 steps)', '1.5-Update Minimum Product Price Data(2 steps)', '2-Update Full Product(5 steps)','4-Update Base Pricing(1 step)']:
            self.obUpdateBasePrice = UpdateBasePrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obUpdateBasePrice.begin_process()
            self.df_product = self.obUpdateBasePrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'UpdateBasePrice')
            if self.success == False or update_action_selected in ['1-Update Minimum Product Data(3 steps)','1.5-Update Minimum Product Price Data(2 steps)', '2-Update Full Product(5 steps)','4-Update Base Pricing(1 step)']:
                return self.success, self.message

        if update_action_selected == 'Update VA Pricing':
            self.obVAPrice = VAPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obVAPrice.begin_process()
            self.df_product = self.obVAPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'VAPrice')
            return self.success, self.message

        if update_action_selected == 'Update GSA Pricing':
            self.obGSAPrice = GSAPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obGSAPrice.begin_process()
            self.df_product = self.obGSAPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'GSAPrice')
            return self.success, self.message

        if update_action_selected == 'Update HTME Pricing':
            self.obHTMEPrice = HTMEPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obHTMEPrice.begin_process()
            self.df_product = self.obHTMEPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'HTMEPrice')
            return self.success, self.message

        if update_action_selected == 'Update ECAT Pricing':
            self.obECATPrice = ECATPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obECATPrice.begin_process()
            self.df_product = self.obECATPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'ECATPrice')
            return self.success, self.message

        if update_action_selected == 'Update FEDMALL Pricing':
            self.obFEDMALLPrice = FEDMALLPrice(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obFEDMALLPrice.begin_process()
            self.df_product = self.obFEDMALLPrice.get_df()
            self.obFileFinder.write_xlsx(self.df_product,'FEDMALLPrice')
            return self.success, self.message

        return False, 'Process not built.'

    def contract_pathway(self, is_testing, contract_selected):
        b_full_run = False
        all_pathways = ['Update Toggles','Process Product Assets']

        self.success, message_or_path = self.obFileFinder.ident_file('Select product data file: '+contract_selected, path = self.start_path)
        if self.success == False:
            return self.success, message_or_path

        self.start_path = (message_or_path.rpartition('\\')[0]).replace('\\\\','\\')
        self.set_perm_file()

        self.success = False
        self.message = 'Post Processing Pathway'

        self.df_product = self.obFileFinder.read_xlsx()

        if contract_selected == 'Update Toggles':

            self.obYNBox = YesNoDialog('Process regardless of change?')
            self.obYNBox.initUI('Toggle dialog.', 'Would you like to update products regardless of change?\n\nThis sets toggles regardless of change.\n')
            if self.obYNBox.yes_selected == True:
                b_full_run = True

            self.obBCRTL = BC_RTL_Object(self.df_product, self.user, self.password, is_testing, full_run = b_full_run)
            self.obBCRTL = BC_RTL_Object(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obBCRTL.begin_process()
            self.df_product = self.obBCRTL.get_df()
            self.obFileFinder.write_xlsx(self.df_product, 'BC_RTL')
            return self.success, self.message

        elif contract_selected == 'Discontinue Products':
            self.obDiscon = DiscontinueObject(self.df_product, self.user, self.password, is_testing)
            self.success, self.message = self.obDiscon.begin_process()
            self.df_product = self.obDiscon.get_df()
            self.obFileFinder.write_xlsx(self.df_product, 'discon')
            return self.success, self.message

        elif contract_selected == 'Process Product Assets':
            self.obAsset = ProcessProductAssetObject(self.df_product, self.user, self.password, is_testing, self.aws_access_key_id, self.aws_secret_access_key)
            self.success, self.message = self.obAsset.begin_process()
            self.df_product = self.obAsset.get_df()
            self.obFileFinder.write_xlsx(self.df_product, 'asset')
            return self.success, self.message

        return False, 'Process not built.'


    ## depricated ##


    def base_data_files(self, table_to_load):
        if table_to_load == 'Category':
            category_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestCategory.xlsx'
            return True, category_base_data_file
        elif table_to_load == 'Manufacturer':
            manufacturer_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestManufacturer.xlsx'
            return True, manufacturer_base_data_file
        elif table_to_load == 'Vendor':
            vendor_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestVendor.xlsx'
            return True, vendor_base_data_file
        elif table_to_load == 'Country':
            country_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestCountry.xlsx'
            return True, country_base_data_file
        elif table_to_load == 'UNSPSC Codes':
            unspsc_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestUNSPSCCode.xlsx'
            return True, unspsc_base_data_file
        elif table_to_load == 'FSC Codes':
            fsc_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestFSCCode.xlsx'
            return True, fsc_base_data_file
        elif table_to_load == 'Hazardous Code':
            hazard_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestHazardCode.xlsx'
            return True, hazard_base_data_file
        elif table_to_load == 'NAICS Code':
            naics_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestNaicsCode.xlsx'
            return True, naics_base_data_file
        elif table_to_load == 'Unit of Issue-Symbol':
            uoi_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestUOISymbol.xlsx'
            return True, uoi_base_data_file
        elif table_to_load == 'Unit of Issue':
            uoi_base_data_file = 'C:\\Users\ImGav\Documents\FranklinYoungFiles\DBIngestion\IngestUOI.xlsx'
            return True, uoi_base_data_file
        else:
            return False, 'No file available.'


## end ##