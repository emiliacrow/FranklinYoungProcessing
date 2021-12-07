# CreatedBy: Emilia Crow
# CreateDate: 20210610
# Updated: 20210610
# CreateFor: Franklin Young International


import pandas
from fuzzywuzzy import fuzz
from Tools.BasicProcess import ReporterObject
from Tools.BasicProcess import BasicProcessObject

from Tools.ProgressBar import AssignCategoryDialog


# to remove duplicated columns
# df = df.loc[:,~df.columns.duplicated()]

class CategoryProcessor(BasicProcessObject):
    req_fields = []
    att_fields = []
    gen_fields = []
    def __init__(self,df_product, user, password, is_testing, proc_to_set):
        self.proc_to_run = proc_to_set
        super().__init__(df_product, user, password, is_testing)
        self.name = 'Category Processor'


    def header_viability(self):
        if self.proc_to_run == 'Category Picker':
            self.req_fields = ['ManufacturerPartNumber', 'ShortDescription']

        if self.proc_to_run == 'Category Training':
            self.req_fields = ['Word1','Word2','Category','IsGood']

        if self.proc_to_run == 'Category Assignment':
            self.req_fields = ['FyProductNumber', 'ManufacturerPartNumber', 'ShortDescription']

        # inital file viability check
        product_headers = set(self.lst_product_headers)
        required_headers = set(self.req_fields)
        overlap = list(required_headers.intersection(product_headers))
        if len(overlap) >= 1:
            self.is_viable = True


    def run_process(self):
        self.obReporter = ReporterObject()
        if self.proc_to_run == 'Category Picker':
            self.success, self.message = self.run_picker_process()
        if self.proc_to_run == 'Category Training':
            self.success, self.message = self.run_training_process()
        if self.proc_to_run == 'Category Assignment':
            self.df_word_cat_associations = self.obDal.get_word_category_associations()
            self.success, self.message = self.run_category_assignment()

        return self.success, self.message

    def run_training_process(self):
        count_of_items = len(self.df_product.index)
        self.dct_counts = {}
        self.collect_return_dfs = []
        self.set_progress_bar(count_of_items, self.name+'[extraction step]')
        p_bar = 0
        good = 0
        bad = 0
        # step one extracts all the categories into category pairs
        for colName, row in self.df_product.iterrows():
            df_line_product = row.to_frame().T
            df_line_product = df_line_product.replace(r'^\s*$', self.np_nan, regex=True)
            df_line_product = df_line_product.dropna(axis=1,how='all')

            if self.line_viability(df_line_product):
                self.ready_report(df_line_product)
                self.obReporter.report_line_viability(True)

                success, return_df_line_product = self.category_evaluation(df_line_product)
                self.obReporter.final_report(success)

            else:
                self.obReporter.report_line_viability(False)
                success, return_df_line_product = self.report_missing_data(df_line_product)

            self.collect_return_dfs.append(return_df_line_product)

            if success:
                good += 1
            else:
                bad += 1

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.return_df_product = self.return_df_product.append(self.collect_return_dfs)
        self.df_product = self.return_df_product
        self.obProgressBarWindow.close()

        self.message = '{2}: {0} Fail, {1} Pass.'.format(bad,good,self.name)

        return self.success, self.message

    def category_evaluation(self, df_line_product):
        self.success = True
        df_return_line_product = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            word1 = row['Word1'].lower()
            word2 = row['Word2'].lower()
            category = row['Category']
            is_good = row['IsGood']
            return_id = self.obDal.set_word_category_associations(word1, word2, category, is_good)

        return True, df_return_line_product


    def category_assignment(self,df_product_line):
        min_score_to_pass = 1

        return_df_line_product = df_product_line.copy()
        for colName, row in df_product_line.iterrows():
            if 'Category' in row:
                # skip if there is already a category
                continue

            description = ''
            description = str(row['ShortDescription'])

            # combine
            if 'LongDescription' in row:
                description = description+' - '+row['LongDescription']

            if 'ProductDescription' in row:
                description = description+' - '+row['ProductDescription']

            # tokenize

            description = description.replace(', ',' ')
            description = description.replace('. ',' ')
            description = description.replace('; ',' ')
            description = description.replace(': ',' ')
            description = description.replace(': ',' ')
            description = description.lower()
            lst_description = description.split()
            lst_description = list(dict.fromkeys(lst_description))
            description = ' '.join(lst_description)

            # for later
            product_number = str(row['ManufacturerPartNumber'])

            # get df with scores
            df_cat_match = self.obDal.get_category_match_desc(description)

            # check if there's a top scorer
            top_score = int((df_cat_match['VoteCount'].to_list())[0])
            # make this somehow related to how much data was tested
            # for example you could use some log(len(lst_description)) or something to make a leveling off value

            if top_score > min_score_to_pass:
                assigned_category = (df_cat_match['CategoryDesc'].to_list())[0]
                return_df_line_product['AssignedCategory'] = assigned_category

            else:
                # give the categories only
                lst_possible_categories = df_cat_match['CategoryDesc'].to_list()

                # display the picker
                self.obCatAssignment = AssignCategoryDialog(product_number, description, lst_possible_categories)

                self.obCatAssignment.exec_()
                result_set = self.obCatAssignment.getReturnSet()

                if 'AssignedCategory' in result_set:
                    assigned_category = result_set['AssignedCategory']
                    if '/' in assigned_category:
                        return_df_line_product['AssignedCategory'] = assigned_category
                    else:
                        return_df_line_product['AssignedCategory'] = ''

                word_1 = ''
                word_2 = ''
                if ('Word1' in result_set) and ('Word2' in result_set):
                    word_1 = result_set['Word1']
                    word_2 = result_set['Word2']

                    if (len(word_1) > 2) and (len(word_2) > 2):
                        if len(assigned_category) - len(assigned_category.replace('/','')) > 2:
                            up_category = assigned_category.rpartition('/')[0]
                            return_id = self.obDal.set_word_category_associations(word_1, word_2, up_category, 1)

                        return_id = self.obDal.set_word_category_associations(word_1, word_2, assigned_category, 1)

        return True, return_df_line_product

    def word_split(self, product_name, product_description):
        dct_match_set = {}
        lst_match_set = []
        combined_options = None
        name_word_options = None
        desc_word_options = None

        # break the word or phrase down to word pairs
        lst_prod_name = product_name.split(' ')
        previous_word = ''
        iteration_count = 0
        for each_word in lst_prod_name:
            if len(each_word) == 0:
                continue

            if iteration_count == 0:
                previous_word = each_word
                iteration_count += 1
                continue

            lst_match_set.append([previous_word,each_word])
            previous_word = each_word
            iteration_count += 1

        # break the word or phrase down to word pairs
        lst_prod_desc = product_description.split(' ')
        previous_word = ''
        iteration_count = 0
        for each_word in lst_prod_desc:
            if len(each_word) == 0:
                continue

            if iteration_count == 0:
                previous_word = each_word
                iteration_count += 1
                continue

            lst_match_set.append([previous_word,each_word])
            previous_word = each_word
            iteration_count += 1

        return lst_match_set


    def identify_word_matches(self,lst_match_set, return_df_line_product):
        self.success == True
        lst_all_word_matches = []
        name_word_combined_options = None
        b_matches = None
        b_set_combined_df = False

        for each_word_pair in lst_match_set:
            # get the options where the two words match up
            b_matches = self.df_word_cat_associations[['word1','word2']].isin(set(each_word_pair))

            if len(b_matches.index) > 0:
                name_word_options = self.df_word_cat_associations.loc[(b_matches['word1'] ==  True) & (b_matches['word2']== True)]
                if b_set_combined_df:
                    # make numbers be numbers
                    name_word_options = name_word_options.astype(int,errors='ignore')
                    lst_all_word_matches.append(name_word_options)
                else:
                    name_word_combined_options = name_word_options.astype(int,errors='ignore')
                    b_set_combined_df = True

        if len(lst_all_word_matches) > 0:
            combined_options = name_word_combined_options.append(lst_all_word_matches)
        else:
            combined_options = name_word_combined_options.copy()

        if len(combined_options.index) > 0:
            # combine all the options
            combined_options = name_word_combined_options.append(lst_all_word_matches)

            # drop dupes if they exists
            combined_options = combined_options.drop_duplicates(subset = ['word1','word2','category'])
            combined_options = combined_options[['is_good','category']]

            # sum the matches
            combined_options['is_good'] = pandas.to_numeric(combined_options['is_good'])
            combined_options['total'] = combined_options.groupby('category',sort=True)['is_good'].transform('sum')

            # sort for the highest score
            combined_options.sort_values(by=['total'], ascending = False, inplace=True)

            pandas.options.display.max_colwidth = 100

            auto_category = combined_options.iloc[0]['category']
            auto_category_count = combined_options.iloc[0]['total']

            return_df_line_product['AutoRecommendation'] =[auto_category]
            return_df_line_product['AutoRecommendationScore'] = [auto_category_count]
        else:
            return_df_line_product['AutoRecommendation'] = ['']
            return_df_line_product['AutoRecommendationScore'] = ['']
            # step two would be to check for single hits and
            # look for those word keys in the rest of the data
            self.obReporter.update_report('Fail','No word match data found')

            self.success = False

        return self.success, return_df_line_product


    def score_by_set(self, phrase_1, phrase_2):
        score = fuzz.token_set_ratio(phrase_1,phrase_2)
        return score

    def score_by_sort(self, phrase_1,phrase_2):
        score = fuzz.token_sort_ratio(phrase_1,phrase_2)
        return score

    def score_by_ratio(self, phrase_1,phrase_2):
        score = fuzz.partial_ratio(phrase_1,phrase_2)
        return score


    def run_category_assignment(self):
        count_of_items = len(self.df_product.index)
        self.lst_parentage = []
        self.collect_return_dfs = []
        self.set_progress_bar(count_of_items, self.name)
        p_bar = 0
        good = 0
        bad = 0
        # step one extracts all the categories into category pairs
        for colName, row in self.df_product.iterrows():
            df_line_product = row.to_frame().T
            df_line_product = df_line_product.replace(r'^\s*$', self.np_nan, regex=True)
            df_line_product = df_line_product.dropna(axis=1,how='all')

            if self.line_viability(df_line_product):
                self.ready_report(df_line_product)
                self.obReporter.report_line_viability(True)

                success, return_df_line_product = self.category_assignment(df_line_product)
                self.obReporter.final_report(success)

                if success:
                    good += 1
                else:
                    bad += 1

            else:
                self.obReporter.final_report(False)
                success, return_df_line_product = self.report_missing_data(df_line_product)

            self.collect_return_dfs.append(return_df_line_product)

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.return_df_product = self.return_df_product.append(self.collect_return_dfs)
        self.df_product = self.return_df_product
        self.obProgressBarWindow.close()

        self.message = '{2}: {0} Fail, {1} Pass.'.format(bad,good,self.name)

        return self.success, self.message


    def hierarchy_display_process(self,df_product_line):
        df_collector_line = df_product_line.copy()
        for colName, row in df_product_line.iterrows():
            new_category_str = row['CategoryName']
            while new_category_str.find("/") != -1:
                new_category_str = new_category_str.replace('/', '§', 1)
                top_level_pair = new_category_str[:new_category_str.find("/")]

                new_category_str = new_category_str[new_category_str.find("§")+1:]
                parent,drop,child = top_level_pair.partition('§')
                if top_level_pair not in self.dct_to_display['Combo']:
                    self.dct_to_display['Parent'].append(parent)
                    self.dct_to_display['Child'].append(child)
                    self.dct_to_display['Combo'].append(top_level_pair)


        return True, df_collector_line


    def set_dict_path(self, heads):
        heads_string = str(heads)
        heads_string = heads_string.replace('\', \'','.')
        heads_string = heads_string.replace('\'','')
        heads_string = heads_string.replace(']','')
        heads_string = heads_string.replace('[','')
        return heads_string

    def dict_generator(self, indict, pre=None):
        pre = pre[:] if pre else []
        if isinstance(indict, dict):
            for key, value in indict.items():
                if isinstance(value, dict):
                    for d in self.dict_generator(value, pre + [key]):
                        yield d
                else:
                    yield pre + [key, value]
        else:
            yield pre + [indict]


    def run_picker_process(self):
        count_of_items = len(self.df_product.index)
        self.return_df_product = pandas.DataFrame(columns=self.df_product.columns)
        self.collect_return_dfs = []
        self.set_progress_bar(count_of_items, self.name)
        p_bar = 0
        good = 0
        bad = 0

        for colName, row in self.df_product.iterrows():
            # this takes one row and builds a df for a single product
            df_line_product = row.to_frame().T
            # this replaces empty string values with nan
            df_line_product = df_line_product.replace(r'^\s*$', self.np_nan, regex=True)
            # this removes all columns with all nan
            df_line_product = df_line_product.dropna(axis=1,how='all')
            if self.line_viability(df_line_product):
                self.ready_report(df_line_product)
                self.obReporter.report_line_viability(True)

                success, return_df_line_product = self.category_management(df_line_product)
                self.obReporter.final_report(success)

            else:
                self.obReporter.final_report(False)
                success, return_df_line_product = self.report_missing_data(df_line_product)

            self.collect_return_dfs.append(return_df_line_product)

            if success:
                good += 1
            else:
                bad += 1

            p_bar+=1
            self.obProgressBarWindow.update_bar(p_bar)

        self.return_df_product = self.return_df_product.append(self.collect_return_dfs)
        self.df_product = self.return_df_product
        self.message = '{2}: {0} Fail, {1} Pass.'.format(bad,good,self.name)

        self.obProgressBarWindow.close()
        return self.success, self.message

    def category_management(self, df_line_product):
        self.success = True
        lst_out_categories = []
        df_collect_product_data = df_line_product.copy()
        for colName, row in df_line_product.iterrows():
            description = str(row['ShortDescription'])

            # combine
            if 'LongDescription' in row:
                description = description+' '+row['LongDescription']

            if 'ProductDescription' in row:
                description = description+' '+row['ProductDescription']

            # tokenize
            lst_description = description.split()
            lst_description = list(dict.fromkeys(lst_description))
            description = ' '.join(lst_description)
            description.replace(',','')

            # for later
            product_number = str(row['ManufacturerPartNumber'])

            # get df with scores
            df_cat_match = self.obDal.get_category_match_desc(description)

            # give the categories only
            lst_possible_categories = df_cat_match['CategoryDesc'].to_list()

            # display the picker
            self.obCatAssignment = AssignCategoryDialog(product_number, description, lst_possible_categories)

            self.obCatAssignment.exec_()
            result_set = self.obCatAssignment.getReturnSet()

            cat_name_selected = ''
            if 'AssignedCategory' in result_set:
                assigned_category = result_set['AssignedCategory']

            word_1 = ''
            word_2 = ''
            if ('Word1' in result_set) and ('Word2' in result_set):
                word_1 = result_set['Word1']
                word_2 = result_set['Word2']

                if (len(word_1) > 2) and (len(word_2) > 2):
                    return_id = self.obDal.set_word_category_associations(word_1, word_2, assigned_category, 1)








        return True, df_collect_product_data



