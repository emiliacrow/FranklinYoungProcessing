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

    sup_fields = ['BCPriceUpdateToggle','BCDataUpdateToggle','VendorIsDiscontinued','FyIsDiscontinued',
                  'UpdateAssets','ECATOnContract','ECATPricingApproved','ECATProductNotes','GSAOnContract','GSAPricingApproved','GSAProductNotes',
                  'HTMEOnContract','HTMEPricingApproved','HTMEProductNotes','INTRAMALLSOnContract','INTRAMALLSPricingApproved','INTRAMALLSProductNotes',
                  'VAOnContract','VAPricingApproved','VAProductNotes','FyProductNotes','ProductNotes',
                  'FyDenyGSAContract', 'FyDenyGSAContractDate','FyDenyVAContract', 'FyDenyVAContractDate',
                  'FyDenyECATContract', 'FyDenyECATContractDate','FyDenyHTMEContractDate', 'FyDenyHTMEContractDate','FyDenyINTRAMALLSContractDate', 'FyDenyINTRAMALLSContractDate']

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

        self.df_ready_products = self.df_ready_products.drop(columns=['db_FyIsDiscontinued'])
        # toggle setup
        self.df_current_toggles = self.obDal.get_toggles_full()

        self.df_ready_products = self.df_ready_products.merge(self.df_current_toggles, how='left', on=['FyProductNumber'])

        self.df_product = pandas.concat([self.df_product, self.df_ready_products], ignore_index = True)


    def remove_private_headers(self):
        private_headers = {'ProductId','ProductId_y','ProductId_x',
                           'ProductPriceId_y','ProductPriceId_x','ProductDescriptionId',
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

            try:
                prod_desc_id = row['ProductDescriptionId']
            except KeyError:
                self.obReporter.update_report('Alert','ProductDescriptionId Missing')
                return False, df_collect_product_base_data


            # check if the product has contract records
            ecat_id = -1
            if 'ECATProductPriceId' in row:
                ecat_id = int(row['ECATProductPriceId'])

            gsa_id = -1
            if 'GSAProductPriceId' in row:
                gsa_id = int(row['GSAProductPriceId'])

            htme_id = -1
            if 'HTMEProductPriceId' in row:
                htme_id = int(row['HTMEProductPriceId'])

            intramalls_id = -1
            if 'INTRAMALLSProductPriceId' in row:
                intramalls_id = int(row['INTRAMALLSProductPriceId'])

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

            # if FyIsDiscontinued is set to Y
            # THEN FyAllowPurchases = 0, FyIsVisible = 0

            # if FyIsDiscontinued is set to N
            # THEN FyAllowPurchases = 1, FyIsVisible = 1


            # if VendorIsDiscontinued is set to Y
            # GSAEligible = 0
            # VAEligible = 0
            # ECATEligible = 0
            # HTMEEligible = 0
            # INTRAMALLSEligible = 0


            is_discontinued = -1
            success, is_discontinued = self.process_boolean(row, 'VendorIsDiscontinued')
            if success:
                df_collect_product_base_data['VendorIsDiscontinued'] = [is_discontinued]
            else:
                is_discontinued = -1

            db_is_discontinued = -1
            success, db_is_discontinued = self.process_boolean(row, 'db_IsDiscontinued')
            if success:
                df_collect_product_base_data['db_IsDiscontinued'] = [db_is_discontinued]
            else:
                db_is_discontinued = -1

            fy_is_discontinued = -1
            success, fy_is_discontinued = self.process_boolean(row, 'FyIsDiscontinued')
            if success:
                df_collect_product_base_data['FyIsDiscontinued'] = [fy_is_discontinued]
            else:
                fy_is_discontinued = -1

            db_fy_is_discontinued = -1
            success, db_fy_is_discontinued = self.process_boolean(row, 'db_FyIsDiscontinued')
            if success:
                df_collect_product_base_data['db_FyIsDiscontinued'] = [db_fy_is_discontinued]
            else:
                db_fy_is_discontinued = -1


            fy_product_notes = ''
            if 'FyProductNotes' in row:
                fy_product_notes = row['FyProductNotes']
                fy_product_notes = fy_product_notes.replace('NULL', '')

            product_notes = ''
            if 'ProductNotes' in row:
                product_notes = row['ProductNotes']
                product_notes = product_notes.replace('NULL', '')


            str_now = datetime.datetime.today().strftime('%d, %b %Y')

            ecat_contract = -1
            ecat_approved = -1
            db_ecat_contract = 0
            db_ecat_approved = 0
            ecat_product_notes = ''
            if ecat_id != -1:
                success, ecat_contract = self.process_boolean(row, 'ECATOnContract')
                if success:
                    df_collect_product_base_data['ECATOnContract'] = [ecat_contract]
                else:
                    ecat_contract = -1


                success, ecat_approved = self.process_boolean(row, 'ECATPricingApproved')
                if success:
                    df_collect_product_base_data['ECATPricingApproved'] = [ecat_approved]
                else:
                    ecat_approved = -1

                if 'ECATProductNotes' in row:
                    ecat_product_notes = row['ECATProductNotes']

                try:
                    db_ecat_contract = int(row['db_ECATOnContract'])
                    db_ecat_approved = int(row['db_ECATPricingApproved'])
                except KeyError:
                    db_ecat_contract = 0
                    db_ecat_approved = 0


                # test if this matches the first condition
                if db_ecat_contract == 1 and ecat_contract == 0 and ecat_approved == 1:
                    mod_number = ''
                    if 'ECATContractModificationNumber' in row:
                        mod_number = str(row['ECATContractModificationNumber'])

                    db_mod_number = str(row['db_ECATModNumber'])

                    approved_price_date = ''
                    if 'ECATApprovedPriceDate' in row:
                        approved_price_date = str(row['ECATApprovedPriceDate'])

                    notes_insert = 'Deleted from contract'
                    if mod_number != '':
                        notes_insert = notes_insert+' with mod '+mod_number

                    if approved_price_date != '':
                        notes_insert = notes_insert + ', approved on ' + approved_price_date.partition(' ')[0]

                    if ecat_product_notes == '':
                        ecat_product_notes = notes_insert
                    else:
                        ecat_product_notes = '{0}, {1}'.format(ecat_product_notes, notes_insert)


            gsa_contract = -1
            gsa_approved = -1
            db_gsa_contract = 0
            db_gsa_approved = 0
            gsa_product_notes = ''
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
                if db_gsa_contract == 1 and gsa_contract == 0 and gsa_approved == 1:
                    mod_number = ''
                    if 'GSAContractModificationNumber' in row:
                        mod_number = str(row['GSAContractModificationNumber'])

                    db_mod_number = str(row['db_GSAModNumber'])

                    approved_price_date = ''
                    if 'GSAApprovedPriceDate' in row:
                        approved_price_date = str(row['GSAApprovedPriceDate'])

                    notes_insert = 'Deleted from contract'
                    if mod_number != '':
                        notes_insert = notes_insert+' with mod '+mod_number

                    if approved_price_date != '':
                        notes_insert = notes_insert + ', approved on ' + approved_price_date.partition(' ')[0]

                    if gsa_product_notes == '':
                        gsa_product_notes = notes_insert
                    else:
                        gsa_product_notes = '{0}, {1}'.format(gsa_product_notes, notes_insert)


            htme_contract = -1
            htme_approved = -1
            db_htme_contract = 0
            db_htme_approved = 0
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
                if db_htme_contract == 1 and htme_contract == 0 and htme_approved == 1:
                    mod_number = ''
                    if 'HTMEContractModificationNumber' in row:
                        mod_number = str(row['HTMEContractModificationNumber'])

                    db_mod_number = str(row['db_HTMEModNumber'])

                    approved_price_date = ''
                    if 'HTMEApprovedPriceDate' in row:
                        approved_price_date = str(row['HTMEApprovedPriceDate'])

                    notes_insert = 'Deleted from contract'
                    if mod_number != '':
                        notes_insert = notes_insert+' with mod '+mod_number

                    if approved_price_date != '':
                        notes_insert = notes_insert + ', approved on ' + approved_price_date.partition(' ')[0]

                    if htme_product_notes == '':
                        htme_product_notes = notes_insert
                    else:
                        htme_product_notes = '{0}, {1}'.format(htme_product_notes, notes_insert)


            intramalls_contract = -1
            intramalls_approved = -1
            db_intramalls_contract = 0
            db_intramalls_approved = 0
            intramalls_product_notes = ''
            if intramalls_id != -1:
                success, intramalls_contract = self.process_boolean(row, 'INTRAMALLSOnContract')
                if success:
                    df_collect_product_base_data['INTRAMALLSOnContract'] = [intramalls_contract]
                else:
                    intramalls_contract = -1

                success, intramalls_approved = self.process_boolean(row, 'INTRAMALLSPricingApproved')
                if success:
                    df_collect_product_base_data['INTRAMALLSPricingApproved'] = [intramalls_approved]
                else:
                    intramalls_approved = -1

                if 'INTRAMALLSProductNotes' in row:
                    intramalls_product_notes = row['INTRAMALLSProductNotes']

                try:
                    db_intramalls_contract = int(row['db_INTRAMALLSOnContract'])
                    db_intramalls_approved = int(row['db_INTRAMALLSPricingApproved'])
                except KeyError:
                    db_intramalls_contract = 0
                    db_intramalls_approved = 0


                # test if this matches the first condition
                if db_intramalls_contract == 1 and intramalls_contract == 0 and intramalls_approved == 1:
                    mod_number = ''
                    if 'INTRAMALLSContractModificationNumber' in row:
                        mod_number = str(row['INTRAMALLSContractModificationNumber'])

                    db_mod_number = str(row['db_INTRAMALLSModNumber'])

                    approved_price_date = ''
                    if 'INTRAMALLSApprovedPriceDate' in row:
                        approved_price_date = str(row['INTRAMALLSApprovedPriceDate'])

                    notes_insert = 'Deleted from contract'
                    if mod_number != '':
                        notes_insert = notes_insert+' with mod '+mod_number

                    if approved_price_date != '':
                        notes_insert = notes_insert + ', approved on ' + approved_price_date.partition(' ')[0]

                    if intramalls_product_notes == '':
                        intramalls_product_notes = notes_insert
                    else:
                        intramalls_product_notes = '{0}, {1}'.format(intramalls_product_notes, notes_insert)



            va_contract = -1
            va_approved = -1
            db_va_contract = 0
            db_va_approved = 0
            va_product_notes = ''
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
                if db_va_contract == 1 and va_contract == 0 and va_approved == 1:
                    mod_number = ''
                    if 'VAContractModificationNumber' in row:
                        mod_number = str(row['VAContractModificationNumber'])

                    db_mod_number = str(row['db_VAModNumber'])

                    approved_price_date = ''
                    if 'VAApprovedPriceDate' in row:
                        approved_price_date = str(row['VAApprovedPriceDate'])

                    notes_insert = 'Deleted from contract'
                    if mod_number != '':
                        notes_insert = notes_insert+' with mod '+mod_number

                    if approved_price_date != '':
                        notes_insert = notes_insert + ', approved on ' + approved_price_date.partition(' ')[0]

                    if va_product_notes == '':
                        va_product_notes = notes_insert
                    else:
                        va_product_notes = '{0}, {1}'.format(va_product_notes, notes_insert)



            # if it's on contract we want to make sure they show
            if (ecat_approved == 1 and ecat_contract == 1) or (gsa_approved == 1 and gsa_contract == 1) or (htme_approved == 1 and htme_contract == 1) or (intramalls_approved == 1 and intramalls_contract == 1) or (va_approved == 1 and va_contract == 1):
                self.obReporter.update_report('Alert','Toggles set based on contract status')
                price_toggle = 1
                df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]

                data_toggle = 1
                df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]

                fy_is_discontinued = 0
                df_collect_product_base_data['FyIsDiscontinued'] = [fy_is_discontinued]


            elif fy_is_discontinued == 1 or is_discontinued == 1:
                price_toggle = 1
                data_toggle = 1
                df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]
                df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
                print("BOING!")

            elif (fy_is_discontinued == -1 or is_discontinued == -1) and (db_fy_is_discontinued == 1 or db_is_discontinued == 1):
                price_toggle = 1
                data_toggle = 1
                df_collect_product_base_data['BCPriceUpdateToggle'] = [price_toggle]
                df_collect_product_base_data['BCDataUpdateToggle'] = [data_toggle]
                print("BING! BONG!")

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


        # at this point we've evaluated all the data
        if (price_toggle != -1 or data_toggle != -1 or is_discontinued != -1 or fy_is_discontinued != -1):
            db_price_toggle = int(row['db_BCPriceUpdateToggle'])
            db_data_toggle = int(row['db_BCPriceUpdateToggle'])
            price_id = -1
            if 'ProductPriceId' in row:
                price_id = int(row['ProductPriceId'])

            success, deny_ecat = self.process_boolean(row, 'FyDenyECATContract')
            if success:
                df_collect_product_base_data['FyDenyECATContract'] = [deny_ecat]
            else:
                deny_ecat = -1

            deny_ecat_date = -1
            if 'FyDenyECATContractDate' in row:
                deny_ecat_date = row['FyDenyECATContractDate']
                try:
                    deny_ecat_date = int(deny_ecat_date)
                    deny_ecat_date = xlrd.xldate_as_datetime(deny_ecat_date, 0)
                except ValueError:
                    deny_ecat_date = str(row['FyDenyECATContractDate'])

                if isinstance(deny_ecat_date, datetime.datetime) == False:
                    try:
                        deny_ecat_date = datetime.datetime.strptime(deny_ecat_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        deny_ecat_date = str(row['FyDenyECATContractDate'])
                        self.obReporter.update_report('Alert','Check FyDenyECATContractDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check FyDenyECATContractDate')

                try:
                    deny_ecat_date = int(row['FyDenyECATContractDate'])
                    deny_ecat_date = (xlrd.xldate_as_datetime(deny_ecat_date, 0)).date()
                except ValueError:
                    deny_ecat_date = str(row['FyDenyECATContractDate'])


            success, deny_gsa = self.process_boolean(row, 'FyDenyGSAContract')
            if success:
                df_collect_product_base_data['FyDenyGSAContract'] = [deny_gsa]
            else:
                deny_gsa = -1

            deny_gsa_date = -1
            if 'FyDenyGSAContractDate' in row:
                deny_gsa_date = row['FyDenyGSAContractDate']
                try:
                    deny_gsa_date = int(deny_gsa_date)
                    deny_gsa_date = xlrd.xldate_as_datetime(deny_gsa_date, 0)
                except ValueError:
                    deny_gsa_date = str(row['FyDenyGSAContractDate'])

                if isinstance(deny_gsa_date, datetime.datetime) == False:
                    try:
                        deny_gsa_date = datetime.datetime.strptime(deny_gsa_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        deny_gsa_date = str(row['FyDenyGSAContractDate'])
                        self.obReporter.update_report('Alert','Check FyDenyGSAContractDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check FyDenyGSAContractDate')

                try:
                    deny_gsa_date = int(row['FyDenyGSAContractDate'])
                    deny_gsa_date = (xlrd.xldate_as_datetime(deny_gsa_date, 0)).date()
                except ValueError:
                    deny_gsa_date = str(row['FyDenyGSAContractDate'])


            success, deny_htme = self.process_boolean(row, 'FyDenyHTMEContract')
            if success:
                df_collect_product_base_data['FyDenyHTMEContract'] = [deny_htme]
            else:
                deny_htme = -1

            deny_htme_date = -1
            if 'FyDenyHTMEContractDate' in row:
                deny_htme_date = row['FyDenyHTMEContractDate']
                try:
                    deny_htme_date = int(deny_htme_date)
                    deny_htme_date = xlrd.xldate_as_datetime(deny_htme_date, 0)
                except ValueError:
                    deny_htme_date = str(row['FyDenyHTMEContractDate'])

                if isinstance(deny_htme_date, datetime.datetime) == False:
                    try:
                        deny_htme_date = datetime.datetime.strptime(deny_htme_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        deny_htme_date = str(row['FyDenyHTMEContractDate'])
                        self.obReporter.update_report('Alert','Check FyDenyHTMEContractDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check FyDenyHTMEContractDate')

                try:
                    deny_htme_date = int(row['FyDenyHTMEContractDate'])
                    deny_htme_date = (xlrd.xldate_as_datetime(deny_htme_date, 0)).date()
                except ValueError:
                    deny_htme_date = str(row['FyDenyHTMEContractDate'])

            success, deny_intramalls = self.process_boolean(row, 'FyDenyINTRAMALLSContract')
            if success:
                df_collect_product_base_data['FyDenyINTRAMALLSContract'] = [deny_intramalls]
            else:
                deny_intramalls = -1

            deny_intramalls_date = -1
            if 'FyDenyINTRAMALLSContractDate' in row:
                deny_intramalls_date = row['FyDenyINTRAMALLSContractDate']
                try:
                    deny_intramalls_date = int(deny_intramalls_date)
                    deny_intramalls_date = xlrd.xldate_as_datetime(deny_intramalls_date, 0)
                except ValueError:
                    deny_intramalls_date = str(row['FyDenyINTRAMALLSContractDate'])

                if isinstance(deny_intramalls_date, datetime.datetime) == False:
                    try:
                        deny_intramalls_date = datetime.datetime.strptime(deny_intramalls_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        deny_intramalls_date = str(row['FyDenyINTRAMALLSContractDate'])
                        self.obReporter.update_report('Alert','Check FyDenyINTRAMALLSContractDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check FyDenyINTRAMALLSContractDate')

                try:
                    deny_intramalls_date = int(row['FyDenyINTRAMALLSContractDate'])
                    deny_intramalls_date = (xlrd.xldate_as_datetime(deny_intramalls_date, 0)).date()
                except ValueError:
                    deny_intramalls_date = str(row['FyDenyINTRAMALLSContractDate'])

            success, deny_va = self.process_boolean(row, 'FyDenyVAContract')
            if success:
                df_collect_product_base_data['FyDenyVAContract'] = [deny_va]
            else:
                deny_va = -1

            deny_va_date = -1
            if 'FyDenyVAContractDate' in row:
                deny_va_date = row['FyDenyVAContractDate']
                try:
                    deny_va_date = int(deny_va_date)
                    deny_va_date = xlrd.xldate_as_datetime(deny_va_date, 0)
                except ValueError:
                    deny_va_date = str(row['FyDenyVAContractDate'])

                if isinstance(deny_va_date, datetime.datetime) == False:
                    try:
                        deny_va_date = datetime.datetime.strptime(deny_va_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        deny_va_date = str(row['FyDenyVAContractDate'])
                        self.obReporter.update_report('Alert','Check FyDenyVAContractDate')
                    except TypeError:
                        self.obReporter.update_report('Alert','Check FyDenyVAContractDate')

                try:
                    deny_va_date = int(row['FyDenyVAContractDate'])
                    deny_va_date = (xlrd.xldate_as_datetime(deny_va_date, 0)).date()
                except ValueError:
                    deny_va_date = str(row['FyDenyVAContractDate'])

            if db_price_toggle != price_toggle or db_data_toggle != data_toggle or db_is_discontinued != is_discontinued or db_fy_is_discontinued != fy_is_discontinued:
                self.obIngester.set_bc_update_toggles(prod_desc_id, price_id, is_discontinued, fy_is_discontinued, price_toggle, data_toggle,
                                                              deny_gsa, deny_gsa_date, deny_va, deny_va_date,
                                                              deny_ecat, deny_ecat_date, deny_htme, deny_htme_date, deny_intramalls, deny_intramalls_date)
            elif self.full_run:
                self.obIngester.set_bc_update_toggles(prod_desc_id, price_id, is_discontinued, fy_is_discontinued, price_toggle, data_toggle,
                                                              deny_gsa, deny_gsa_date, deny_va, deny_va_date,
                                                              deny_ecat, deny_ecat_date, deny_htme, deny_htme_date, deny_intramalls, deny_intramalls_date)


        if (update_asset != -1):
            self.obIngester.set_update_asset(product_id, update_asset)


        if (ecat_contract != -1 or ecat_approved != -1 or ecat_product_notes != ''):
            if db_ecat_contract != ecat_contract or db_ecat_approved != ecat_approved:
                self.obIngester.set_ecat_toggles(ecat_id, fy_product_number, ecat_contract, ecat_approved, ecat_product_notes)
            elif self.full_run:
                self.obIngester.set_ecat_toggles(ecat_id, fy_product_number, ecat_contract, ecat_approved, ecat_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to ECAT toggles')

        if (gsa_contract != -1 or gsa_approved != -1 or gsa_product_notes != ''):
            if db_gsa_contract != gsa_contract or db_gsa_approved != gsa_approved:
                self.obIngester.set_gsa_toggles(gsa_id, fy_product_number, gsa_contract, gsa_approved, gsa_product_notes)
            elif self.full_run:
                self.obIngester.set_gsa_toggles(gsa_id, fy_product_number, gsa_contract, gsa_approved, gsa_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to GSA toggles')

        if (htme_contract != -1 or htme_approved != -1 or htme_product_notes != ''):
            if db_htme_contract != htme_contract or db_htme_approved != htme_approved:
                self.obIngester.set_htme_toggles(htme_id, fy_product_number, htme_contract, htme_approved, htme_product_notes)
            elif self.full_run:
                self.obIngester.set_htme_toggles(htme_id, fy_product_number, htme_contract, htme_approved, htme_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to HTME toggles')

        if (intramalls_contract != -1 or intramalls_approved != -1 or intramalls_product_notes != ''):
            if db_intramalls_contract != intramalls_contract or db_intramalls_approved != intramalls_approved:
                self.obIngester.set_intramalls_toggles(intramalls_id, fy_product_number, intramalls_contract, intramalls_approved, intramalls_product_notes)
            elif self.full_run:
                self.obIngester.set_intramalls_toggles(intramalls_id, fy_product_number, intramalls_contract, intramalls_approved, intramalls_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to INTRAMALLS toggles')

        if (va_contract != -1 or va_approved != -1 or va_product_notes != ''):
            if db_va_contract != va_contract or db_va_approved != va_approved:
                self.obIngester.set_va_toggles(va_id, fy_product_number, va_contract, va_approved, va_product_notes)
            elif self.full_run:
                self.obIngester.set_va_toggles(va_id, fy_product_number, va_contract, va_approved, va_product_notes)
            else:
                self.obReporter.update_report('Alert', 'No change to VA toggles')

        if (product_notes != '' or fy_product_notes != ''):
            self.obIngester.set_product_notes(prod_desc_id, fy_product_notes, price_id, product_notes)


        return True, df_collect_product_base_data


    def trigger_ingest_cleanup(self):
        self.obIngester.set_bc_update_toggles_cleanup()
        self.obIngester.set_update_asset_cleanup()
        self.obIngester.set_ecat_toggles_cleanup()
        self.obIngester.set_gsa_toggles_cleanup()
        self.obIngester.set_htme_toggles_cleanup()
        self.obIngester.set_intramalls_toggles_cleanup()
        self.obIngester.set_va_toggles_cleanup()
        self.obIngester.set_product_notes_cleanup()



## end ##