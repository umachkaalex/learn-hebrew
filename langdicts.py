### Create language texts
lang_dict = {'RUS': {
                     ### add row block 
                     'hebrew': 'введите слово на иврите: ',                     
                     'translation': 'введите перевод: ',                     
                     'translitiration': 'введите транслитерацию: ',
                     'genus': 'введите род (муж, жен): ',
                     'genus_list': ['муж', 'жен'],
                     'type': 'введите часть речи (сущ, глаг, мест, прилаг, нареч, вопрос, союз): ',
                     'type_list': ['сущ', 'глаг', 'мест', 'прилаг',
                                   'нареч', 'вопрос', 'союз', 'доп'],
                     'plural': 'введите число (мн, ед): ',
                     'plural_list': ['мн', 'ед'],
                     'genus_list': ['муж', 'жен'],
                     'type_error': 'нет такой части речи',
                     'genus_error': 'нет такого рода',
                     'plural_error': 'нет такого числа',
                     'finish_add': 'закончили',
                     'check_add': 'проверяем: ',
                     'dupl_err_txt': ['слово ', ' есть в словаре']  
                     }}

lang_dict_check = {'RUS': {
                           ### check knowledge
                           'transl_word': 'переведите слово: ',
                           }}

lang_noun = {'RUS': {'m-sl': 'муж. род/един. число: ',
                     'm-pl': 'муж. род/мн. число: ',
                     'w-sl': 'жен. род/един. число: ',
                     'w-pl': 'жен. род/мн. число: ',
                     'translation': 'перевод',
                     'translitiration': 'транслитерация',
                     }}
