# CreatedBy: Emilia Crow
# CreateDate: 20210927
# Updated: 20210927
# CreateFor: Franklin Young International

import pandas
import datetime
import numpy as np

from Tools.BasicProcess import BasicProcessObject


class BigCommerceRTLObject(BasicProcessObject):
    req_fields = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber']

    sup_fields = ['BCPriceUpdateToggle','BCDataUpdateToggle','IsDiscontinued','AllowPurchases','IsVisible',
                  'UpdateAssets','ECATOnContract','ECATPricingApproved','HTMETOnContract','HTMEPricingApproved',
                  'GSAOnContract','GSAPricingApproved','VAOnContract','VAPricingApproved','FyProductNotes']
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, full_run=False):
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Toggle Lifter'
        self.full_run = full_run

    def batch_preprocessing(self):
        self.remove_private_headers()
        self.define_new()
        self.assign_current_toggles()

    def assign_current_toggles(self):
        self.df_ready_products = self.df_product[(self.df_product['Filter'] == 'Ready')].copy()
        self.df_product = self.df_product[(self.df_product['Filter'] != 'Ready')]

        self.df_ready_products = self.df_ready_products.drop(columns=['Filter','db_IsDiscontinued'])
        # toggle setup
        self.df_current_toggles = self.obDal.get_toggles_full()

        self.df_top_toggles = self.df_current_toggles.drop_duplicates(subset = ['FyProductNumber'])

        drop_values = ['db_IsDiscontinued','db_AllowPurchases',
                       'db_IsVisible', 'db_BCDataUpdateToggle', 'db_BCPriceUpdateToggle',
                       'db_ECATOnContract', 'db_ECATModNumber', 'db_ECATPricingApproved',
                       'db_HTMEOnContract', 'db_HTMEModNumber','db_HTMEPricingApproved',
                       'db_GSAOnContract', 'db_GSAModNumber','db_GSAPricingApproved',
                       'db_VAOnContract', 'db_VAModNumber','db_VAPricingApproved']
        self.df_top_toggles = self.df_top_toggles.drop(columns=drop_values)

        match_headers = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber']
        self.df_current_toggles['Filter'] = 'Update'
        self.df_ready_products = self.df_ready_products.merge(self.df_current_toggles, how='left', on=match_headers)

        self.df_ready_products.loc[(self.df_ready_products['Filter'] != 'Update'), 'Filter'] = 'Fail'

        match_headers = ['FyCatalogNumber','ManufacturerName', 'ManufacturerPartNumber','FyProductNumber','VendorName','VendorPartNumber']
        self.df_top_toggles['IsTopProduct'] = 'Y'

        self.df_ready_products = self.df_ready_products.merge(self.df_top_toggles, how='left', on=match_headers)

        self.df_ready_products.loc[(self.df_ready_products['IsTopProduct'] != 'Y'), 'IsTopProduct'] = 'N'

        self.df_ready_products = self.df_ready_products.sort_values(by=['FyProductNumber','IsTopProduct'], ascending= False)

        self.df_product = pandas.concat([self.df_product, self.df_ready_products], ignore_index = True)

        # self.df_product = self.df_product.drop_duplicates(subset = ['FyProductNumber','VendorPartNumber_y']


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId_y','ProductPriceId_x',
                           'VendorId','VendorId_x','VendorId_y',
                           'CategoryId','CategoryId_x','CategoryId_y',
                           'Report','Filter'}
        current_headers = set(self.df_product.columns)
        remove_headers = list(current_headers.intersection(private_headers))
        if remove_headers != []:
            self.df_product = self.df_product.drop(columns=remove_headers)


    def filter_check_in(self, row):
        filter_options = ['Base Pricing', 'New', 'Partial', 'Possible Duplicate', 'Ready', 'case_1','case_4']

        if row['Filter'] == 'New':
            self.obReporter.update_report('Alert', 'Passed filtering as a new product but not processed')
            return False

        elif row['Filter'] == 'Partial':
            self.obReporter.update_report('Alert', 'Passed filtering as partial product')
            return False

        elif row['Filter'] in ['Ready', 'Update', 'Base Pricing']:
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
        for colName, row in df_line_product.iterrows():
            if self.filter_check_in(row) == False:
                return False, df_collect_product_base_data

        success, return_df_line_product = self.process_changes(df_collect_product_base_data)

        return success, return_df_line_product


    def process_changes(self, df_collect_product_base_data):
        
        for colName, row in df_collect_product_base_data.iterrows():
            # these must exist
            try:
                product_id = row['ProductId']
            except KeyError:
                self.obReporter.update_report('Alert','ProductId Missing')
                return False, df_collect_product_base_data

            try:
                price_id = row['ProductPriceId']
            except KeyError:
                self.obReporter.update_report('Alert','ProductPriceId Missing')
                return False, df_collect_product_base_data

            try:
                base_id = row['BaseProductPriceId']
            except KeyError:
                self.obReporter.update_report('Alert','BaseProductPriceId Missing')
                return False, df_collect_product_base_data

            # check if the product has contract records
            ecat_id = -1
            if 'ECATProductPriceId' in row:
                ecat_id = int(row['ECATProductPriceId'])

            htme_id = -1
            if 'HTMEProductPriceId' in row:
                htme_id = int(row['HTMEProductPriceId'])

            gsa_id = -1
            if 'GSAProductPriceId' in row:
                gsa_id = int(row['GSAProductPriceId'])

            va_id = -1
            if 'VAProductPriceId' in row:
                va_id = int(row['VAProductPriceId'])

            fy_product_number = row['FyProductNumber']
            vendor_part_number = str(row['VendorPartNumber'])

            update_asset = -1
            success, update_asset = self.process_boolean(row, 'UpdateAssets')
            if success:
                df_collect_product_base_data['UpdateAssets'] = [update_asset]
            else:
                update_asset = -1

            is_discontinued = -1
            success, is_discontinued = self.process_boolean(row, 'IsDiscontinued')
            if success:
                df_collect_product_base_data['IsDiscontinued'] = [is_discontinued]
            else:
                is_discontinued = -1


            db_is_discontinued = int(row['db_IsDiscontinued'])

            try:
                db_allow_purchases = int(row['db_AllowPurchases'])
            except:
                print(row)
                db_allow_purchases = -1

            fy_product_notes = ''
            if 'FyProductNotes' in row:
                fy_product_notes = row['FyProductNotes']
                fy_product_notes = fy_product_notes.replace('NULL', '')
                fy_product_notes = fy_product_notes.replace(';', ',')


            str_now = datetime.datetime.today().strftime('%d, %b %Y')

            ecat_contract = -1
            ecat_approved = -1
            db_ecat_contract = 0
            db_ecat_approved = 0
            ecat_pending_del_flag = -1
            ecat_product_notes = ''
            if ecat_id != -1:
                success, ecat_contract = self.process_boolean(row, 'ECATOnContract')
                if success:
                    df_collect_product_base_data['ECATOnContract'] = [ecat_contract]

                success, ecat_approved = self.process_boolean(row, 'ECATPricingApproved')
                if success:
                    df_collect_product_base_data['ECATPricingApproved'] = [ecat_approved]

                if 'ECATProductNotes' in row:
                    ecat_product_notes = row['ECATProductNotes']

                try:
                    db_ecat_contract = int(row['db_ECATOnContract'])
                    db_ecat_approved = int(row['db_ECATPricingApproved'])
                except KeyError:
                    db_ecat_contract = 0
                    db_ecat_approved = 0


                # test if this matches the first condition
                # not discontinued, gets pending if db discontinued and db contracted
                if db_is_discontinued == 0 and is_discontinued == 1 and db_ecat_contract == 1 and db_ecat_approved == 1 and ecat_approved != 1:
                    ecat_pending_del_flag = 1
                    is_discontinued = 0
                    ecat_approved = 0

                    if 'ending contract deletion,' not in fy_product_notes:
                        if fy_product_notes == '':
                            fy_product_notes = 'Pending contract deletion, {0}'.format(str_now)
                        else:
                            fy_product_notes = '{0}, pending contract deletion, {1}'.format(fy_product_notes, str_now)

                # if we're actually deleting from contract, we can add the notes
                elif db_ecat_contract == 1 and ecat_contract == 0 and ecat_approved == 1:
                    mod_number = str(row['db_ECATModNumber'])
                    ecat_pending_del_flag = 0
                    if ecat_product_notes == '':
                        ecat_product_notes = 'Deleted from contract, {0}'.format(str_now)
                    else:
                        ecat_product_notes = '{0}, Deleted from contract, {1}'.format(ecat_product_notes, str_now)


            htme_contract = -1
            htme_approved = -1
            db_htme_contract = 0
            db_htme_approved = 0
            htme_pending_del_flag = -1
            htme_product_notes = ''
            if htme_id != -1:
                success, htme_contract = self.process_boolean(row, 'HTMETOnContract')
                if success:
                    df_collect_product_base_data['HTMETOnContract'] = [htme_contract]
                else:
                    htme_contract = -1

                success, htme_approved = self.process_boolean(row, 'HTMEPricingApproved')
                if success:
                    df_collect_product_base_data['HTMEPricingApproved'] = [htme_approved]
                else:
                    htme_approved = -1

                if 'HTMEProductNotes' in row:
                    htme_product_notes = row['HTMEProductNotes']

                try:
                    db_htme_contract = int(row['db_HTMEOnContract'])
                    db_htme_approved = int(row['db_HTMEPricingApproved'])
                except KeyError:
                    db_htme_contract = 0
                    db_htme_approved = 0


                # test if this matches the first condition
                # not discontinued, gets pending if db discontinued and db contracted
                if db_is_discontinued == 0 and is_discontinued == 1 and db_htme_contract == 1 and db_htme_approved == 1 and htme_approved != 1:
                    htme_pending_del_flag = 1
                    is_discontinued = 0
                    htme_approved = 0

                    if 'ending contract deletion,' not in fy_product_notes:
                        if fy_product_notes == '':
                            fy_product_notes = 'Pending contract deletion, {0}'.format(str_now)
                        else:
                            fy_product_notes = '{0}, pending contract deletion, {1}'.format(fy_product_notes, str_now)

                # if we're actually deleting from contract, we can add the notes
                elif db_htme_contract == 1 and htme_contract == 0 and htme_approved == 1:
                    mod_number = str(row['db_HTMEModNumber'])
                    if htme_product_notes == '':
                        htme_product_notes = 'Deleted from contract, {0}'.format(str_now)
                    else:
                        htme_product_notes = '{0}, Deleted from contract, {1}'.format(htme_product_notes, str_now)


            gsa_contract = -1
            gsa_approved = -1
            db_gsa_contract = 0
            db_gsa_approved = 0
            gsa_product_notes = ''
            gsa_pending_del_flag = -1
            if gsa_id != -1:
                success, gsa_contract = self.process_boolean(row, 'GSAOnContract')
                if success:
                    df_collect_product_base_data['GSAOnContract'] = [gsa_contract]
                else:
                    gsa_contract = -1

                success, gsa_approved = self.process_boolean(row, 'GSAPricingApproved')
                if success:
                    df_collect_product_base_data['GSAPricingApproved'] = [gsa_approved]
                else:
                    gsa_approved = -1

                if 'GSAProductNotes' in row:
                    gsa_product_notes = row['GSAProductNotes']

                try:
                    db_gsa_contract = int(row['db_GSAOnContract'])
                    db_gsa_approved = int(row['db_GSAPricingApproved'])
                except KeyError:
                    db_gsa_contract = 0
                    db_gsa_approved = 0


                # test if this matches the first condition
                # not discontinued, gets pending if db discontinued and db contracted
                if db_is_discontinued == 0 and is_discontinued == 1 and db_gsa_contract == 1 and db_gsa_approved == 1 and gsa_approved != 1:
                    gsa_pending_del_flag = 1
                    is_discontinued = 0
                    ecat_approved = 0

                    if 'ending contract deletion,' not in fy_product_notes:
                        if fy_product_notes == '':
                            fy_product_notes = 'Pending contract deletion, {0}'.format(str_now)
                        else:
                            fy_product_notes = '{0}, pending contract deletion, {1}'.format(fy_product_notes, str_now)

                # if we're actually deleting from contract, we can add the notes
                elif db_gsa_contract == 1 and gsa_contract == 0 and gsa_approved == 1:
                    mod_number = str(row['db_GSAModNumber'])
                    if gsa_product_notes == '':
                        gsa_product_notes = 'Deleted from contract, {0}'.format(str_now)
                    else:
                        gsa_product_notes = '{0}, Deleted from contract, {1}'.format(gsa_product_notes, str_now)


            va_contract = -1
            va_approved = -1
            db_va_contract = 0
            db_va_approved = 0
            va_product_notes = ''
            va_pending_del_flag = -1
            if va_id != -1:
                success, va_contract = self.process_boolean(row, 'VAOnContract')
                if success:
                    df_collect_product_base_data['VAOnContract'] = [va_contract]
                else:
                    va_contract = -1

                success, va_approved = self.process_boolean(row, 'VAPricingApproved')
                if success:
                    df_collect_product_base_data['VAPricingApproved'] = [va_approved]
                else:
                    va_approved = -1

                if 'VAProductNotes' in row:
                    va_product_notes = row['VAProductNotes']

                try:
                    db_va_contract = int(row['db_VAOnContract'])
                    db_va_approved = int(row['db_VAPricingApproved'])
                except KeyError:
                    db_va_contract = 0
                    db_va_approved = 0


                # test if this matches the first condition
                # not discontinued, gets pending if db discontinued and db contracted
                if db_is_discontinued == 0 and is_discontinued == 1 and db_va_contract == 1 and db_va_approved == 1 and va_approved != 1:
                    va_pending_del_flag = 1
                    is_discontinued = 0
                    ecat_approved = 0

                    if 'ending contract deletion,' not in fy_product_notes:
                        if fy_product_notes == '':
                            fy_product_notes = 'Pending contract deletion, {0}'.format(str_now)
                        else:
                            fy_product_notes = '{0}, pending contract deletion, {1}'.format(fy_product_notes, str_now)

                # if we're actually deleting from contract, we can add the notes
                elif db_va_contract == 1 and va_contract == 0 and va_approved == 1:
                    mod_number = str(row['db_VAModNumber'])
                    if va_product_notes == '':
                        va_product_notes = 'Deleted from contract, {0}'.format(str_now)
                    else:
                        va_product_notes = '{0}, Deleted from contract, {1}'.format(va_product_notes, str_now)


            # if it's on contract we want to make sure they show
            if (ecat_approved == 1 and ecat_contract == 1) or (htme_approved == 1 and htme_contract == 1) or (gsa_approved == 1 and gsa_contract == 1) or (va_approved == 1 and va_contract == 1):
                price_toggle = 1
                df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]

                is_discontinued = 0
                df_collect_product_base_data['IsDiscontinued'] = [is_discontinued]

                allow_purchases = 1
                df_collect_product_base_data['AllowPurchases'] = [allow_purchases]

                is_visible = 1
                df_collect_product_base_data['IsVisible'] = [is_visible]

                data_toggle = -1
                if (update_asset != -1):
                    data_toggle = 1
                    df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]

                else:
                    success, data_toggle = self.process_boolean(row, 'BCDataUpdateToggle')
                    if success:
                        df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
                    else:
                        data_toggle = -1

            else:
                success, price_toggle = self.process_boolean(row, 'BCPriceUpdateToggle')
                if success:
                    df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]
                else:
                    price_toggle = -1

                success, data_toggle = self.process_boolean(row, 'BCDataUpdateToggle')
                if success:
                    df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
                else:
                    data_toggle = -1

                allow_purchases = -1
                success, allow_purchases = self.process_boolean(row, 'AllowPurchases')
                if success:
                    df_collect_product_base_data['AllowPurchases'] = [allow_purchases]
                else:
                    allow_purchases = -1

                is_visible = -1
                success, is_visible = self.process_boolean(row, 'IsVisible')
                if success:
                    df_collect_product_base_data['IsVisible'] = [is_visible]
                else:
                    is_visible = -1



        # at this point we've evaluated all the data
        if (price_toggle != -1 or data_toggle != -1):
            # this needs to be better
            if str(row['IsTopProduct']) == 'Y':
                db_price_toggle = int(row['db_BCPriceUpdateToggle'])
                db_data_toggle = int(row['db_BCPriceUpdateToggle'])
                if db_price_toggle != price_toggle or db_data_toggle != data_toggle:
                    self.obIngester.set_bc_update_toggles(price_id, fy_product_number, price_toggle, data_toggle)
                elif self.full_run:
                    self.obIngester.set_bc_update_toggles(price_id, fy_product_number, price_toggle, data_toggle)
                else:
                    self.obReporter.update_report('Alert', 'No change to BC toggles')

        if (is_discontinued != -1 or allow_purchases != -1):
            if str(row['IsTopProduct']) == 'Y':
                if db_is_discontinued != is_discontinued or db_allow_purchases != allow_purchases:
                    self.obIngester.set_is_discon_allow_purchase(price_id, fy_product_number, is_discontinued, allow_purchases)

                elif self.full_run:
                    self.obIngester.set_is_discon_allow_purchase(price_id, fy_product_number, is_discontinued, allow_purchases)
                else:
                    self.obReporter.update_report('Alert', 'No change to discon/purchase toggles')

            else:
                if db_is_discontinued != is_discontinued or db_allow_purchases != 0:
                    allow_purchases = 0
                    self.obReporter.update_report('Alert','Product is not top, AllowPurchase overridden')
                    self.obIngester.set_is_discon_allow_purchase(price_id, fy_product_number, is_discontinued, allow_purchases)
                elif self.full_run:
                    allow_purchases = 0
                    self.obReporter.update_report('Alert','Product is not top, AllowPurchase overridden')
                    self.obIngester.set_is_discon_allow_purchase(price_id, fy_product_number, is_discontinued, allow_purchases)
                else:
                    self.obReporter.update_report('Alert', 'No change to discon/purchase toggles')


        if (is_visible != -1):
            db_is_visible = int(row['db_IsVisible'])
            if db_is_visible != is_visible:
                self.obIngester.set_is_visible(base_id, is_visible)
            elif self.full_run:
                self.obIngester.set_is_visible(base_id, is_visible)
            else:
                self.obReporter.update_report('Alert', 'No change to is visible')

        if (update_asset != -1):
            self.obIngester.set_update_asset(product_id, update_asset)


        if (ecat_contract != -1 or ecat_approved != -1):
            if db_ecat_contract != ecat_contract or db_ecat_approved != ecat_approved:
                self.obIngester.set_ecat_toggles(ecat_id, fy_product_number, ecat_contract, ecat_approved, ecat_pending_del_flag, ecat_product_notes)
            elif self.full_run:
                self.obIngester.set_ecat_toggles(ecat_id, fy_product_number, ecat_contract, ecat_approved, ecat_pending_del_flag, ecat_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to ECAT toggles')

        if (htme_contract != -1 or htme_approved != -1):
            if db_htme_contract != htme_contract or db_htme_approved != htme_approved:
                self.obIngester.set_htme_toggles(htme_id, fy_product_number, htme_contract, htme_approved, htme_pending_del_flag, htme_product_notes)
            elif self.full_run:
                self.obIngester.set_htme_toggles(htme_id, fy_product_number, htme_contract, htme_approved, htme_pending_del_flag, htme_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to HTME toggles')

        if (gsa_contract != -1 or gsa_approved != -1):
            if db_gsa_contract != gsa_contract or db_gsa_approved != gsa_approved:
                self.obIngester.set_gsa_toggles(gsa_id, fy_product_number, gsa_contract, gsa_approved, gsa_pending_del_flag, gsa_product_notes)
            elif self.full_run:
                self.obIngester.set_gsa_toggles(gsa_id, fy_product_number, gsa_contract, gsa_approved, gsa_pending_del_flag, gsa_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to GSA toggles')

        if (va_contract != -1 or va_approved != -1):
            if db_va_contract != va_contract or db_va_approved != va_approved:
                self.obIngester.set_va_toggles(va_id, fy_product_number, va_contract, va_approved, va_pending_del_flag, va_product_notes)
            elif self.full_run:
                self.obIngester.set_va_toggles(va_id, fy_product_number, va_contract, va_approved, va_pending_del_flag, va_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to VA toggles')

        if (fy_product_notes != ''):
            self.obIngester.set_product_notes(price_id, fy_product_notes)


        return True, df_collect_product_base_data

    def trigger_ingest_cleanup(self):
        self.obIngester.set_bc_update_toggles_cleanup()
        self.obIngester.set_is_discon_allow_purchase_cleanup()
        self.obIngester.set_is_visible_cleanup()
        self.obIngester.set_update_asset_cleanup()
        self.obIngester.set_ecat_toggles_cleanup()
        self.obIngester.set_htme_toggles_cleanup()
        self.obIngester.set_gsa_toggles_cleanup()
        self.obIngester.set_va_toggles_cleanup()
        self.obIngester.set_product_notes_cleanup()

## end ##