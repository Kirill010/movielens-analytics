import re
import csv
from collections import Counter
from datetime import datetime
import statistics
import pytest
import os
import tempfile
from unittest.mock import Mock, patch
import requests
from bs4 import BeautifulSoup
    
class Ratings:
    """
    Analyzing data from ratings.csv
    """
    def __init__(self, path_to_the_file):
        """
        Put here any fields that you think you will need.
        """
        self.userIds = []
        self.movieIds = []
        self.ratings = []
        self.timestamps = []
        self.years = []
        
        # Загружаем данные из ratings.csv
        with open(path_to_the_file) as file:
            next(file)
            lines = file.readlines()
            for line in lines:
                line_list = line.strip().split(',')
                self.userIds.append(int(line_list[0]))
                self.movieIds.append(int(line_list[1]))
                self.ratings.append(float(line_list[2]))
                self.timestamps.append(int(line_list[3]))
                self.years.append(datetime.fromtimestamp(int(line_list[3])).year)
        
        # Загружаем movies для сопоставления movieId с названиями
        self.movies_data = self._load_movies_data()
        self.data = list(self.load_data(path_to_the_file))

    def load_data(self, path):
        try:
            with open(path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  
                for line in reader:
                    if line:
                        yield {
                            'userId': int(line[0]),
                            'movieId': int(line[1]),
                            'rating': float(line[2]),
                            'timestamp': int(line[3]),
                        }
        except FileNotFoundError as e:
            print(f"File not found: {e}")
    
    def _load_movies_data(self):
        """Загружаем данные о фильмах для сопоставления"""
        movies_data = {}
        # Ищем файл movies.csv в той же директории, что и ratings.csv
        ratings_dir = os.path.dirname('ml-latest-small/ratings.csv')
        movies_path = os.path.join(ratings_dir, 'ml-latest-small/movies.csv')
        
        try:
            with open('ml-latest-small/movies.csv', 'r', encoding='utf-8') as file:
                next(file)
                for line in file:
                    parts = line.strip().split(',')
                    if len(parts) >= 2:
                        movie_id = parts[0].strip()
                        title = parts[1].strip().strip('"')
                        movies_data[int(movie_id)] = title
        except FileNotFoundError:
            print("Файл movies.csv не найден. Будут использоваться только movieId.")
        
        return movies_data
    
    class Movies:
        def __init__(self, ratings_instance):
            self.ratings_instance = ratings_instance
            
        def get_average(self, ratings):
            """Вычисление среднего значения"""
            if not ratings:
                return None
            return sum(ratings) / len(ratings)

        def get_median(self, ratings):
            """Вычисление медианы"""
            if not ratings:
                return None
            return statistics.median(ratings)
        
        def dist_by_year(self):
            """
            The method returns a dict where the keys are years and the values are counts. 
            Sort it by years ascendingly. You need to extract years from timestamps.
            """
            counts = {}
            for timestamp in self.ratings_instance.timestamps:
                year = datetime.fromtimestamp(timestamp).year
                counts[year] = counts.get(year, 0) + 1
            
            return dict(sorted(counts.items()))
        
        def dist_by_rating(self):
            """
            The method returns a dict where the keys are ratings and the values are counts.
            Sort it by ratings ascendingly.
            """
            counts = {}
            for rating in self.ratings_instance.ratings:
                counts[rating] = counts.get(rating, 0) + 1
            
            return dict(sorted(counts.items()))
        
        def top_by_num_of_ratings(self, n):
            """
            The method returns top-n movies by the number of ratings. 
            It is a dict where the keys are movie titles and the values are numbers.
            Sort it by numbers descendingly.
            """
            ratings_counts = {}
            for movie_id in self.ratings_instance.movieIds:
                ratings_counts[movie_id] = ratings_counts.get(movie_id, 0) + 1

            movie_metrics = {}
            for movie_id, count in ratings_counts.items():
                title = self.ratings_instance.movies_data.get(movie_id)
                if title:
                    movie_metrics[title] = count

            sorted_movies = dict(sorted(movie_metrics.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_movies.items())[:n])
        
        def top_by_ratings(self, n, metric='average'):
            """
            The method returns top-n movies by the average or median of the ratings.
            It is a dict where the keys are movie titles and the values are metric values.
            Sort it by metric descendingly.
            The values should be rounded to 2 decimals.
            """
            movie_ratings = {}
            for i in range(len(self.ratings_instance.movieIds)):
                movie_id = self.ratings_instance.movieIds[i]
                rating = self.ratings_instance.ratings[i]
                if movie_id not in movie_ratings:
                    movie_ratings[movie_id] = []
                movie_ratings[movie_id].append(rating)

            movie_metrics = {}
            for movie_id, ratings in movie_ratings.items():
                title = self.ratings_instance.movies_data.get(movie_id)
                if title and ratings:
                    if metric == 'average':
                        value = self.get_average(ratings)
                    elif metric == 'median':
                        value = self.get_median(ratings)
                    else:
                        value = self.get_average(ratings)
                    if value is not None:
                        movie_metrics[title] = round(value, 2)

            sorted_metrics = dict(sorted(movie_metrics.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_metrics.items())[:n])
        
        def top_controversial(self, n, min_num_of_ratings):
            """
            The method returns top-n movies by the variance of the ratings.
            It is a dict where the keys are movie titles and the values are the variances.
            Sort it by variance descendingly.
            The values should be rounded to 2 decimals.
            """
            movie_ratings = {}
            for i in range(len(self.ratings_instance.movieIds)):
                movie_id = self.ratings_instance.movieIds[i]
                rating = self.ratings_instance.ratings[i]
                if movie_id not in movie_ratings:
                    movie_ratings[movie_id] = []
                movie_ratings[movie_id].append(rating)
            
            variances = {}
            for movie_id, ratings in movie_ratings.items():
                if len(ratings) >= min_num_of_ratings:
                    title = self.ratings_instance.movies_data.get(movie_id)
                    if title and len(ratings) > 1:
                        try:
                            variance = statistics.variance(ratings)
                            variances[title] = round(variance, 2)
                        except statistics.StatisticsError:
                            continue
            
            sorted_by_var = dict(sorted(variances.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_by_var.items())[:n])

    class Users(Movies):
        """
        In this class, three methods should work. 
        The 1st returns the distribution of users by the number of ratings made by them.
        The 2nd returns the distribution of users by average or median ratings made by them.
        The 3rd returns top-n users with the biggest variance of their ratings.
        Inherit from the class Movies. Several methods are similar to the methods from it.
        """
        def __init__(self, ratings_instance):
            super().__init__(ratings_instance)
            self.ratings_instance = ratings_instance
        
        def dist_by_num_of_ratings(self):
            """Распределение пользователей по количеству оценок"""
            counts = {}
            for user_id in self.ratings_instance.userIds:
                counts[user_id] = counts.get(user_id, 0) + 1
            
            return dict(sorted(counts.items()))
        
        def top_by_num_of_ratings(self, n):
            """Топ-n пользователей по количеству оценок"""
            counts = {}
            for user_id in self.ratings_instance.userIds:
                counts[user_id] = counts.get(user_id, 0) + 1
            
            sorted_counts = dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_counts.items())[:n])
        
        def top_by_ratings(self, n, metric='average'):
            """Топ-n пользователей по средней/медианной оценке"""
            user_ratings = {}
            for i in range(len(self.ratings_instance.userIds)):
                user_id = self.ratings_instance.userIds[i]
                rating = self.ratings_instance.ratings[i]
                if user_id not in user_ratings:
                    user_ratings[user_id] = []
                user_ratings[user_id].append(rating)

            user_metrics = {}
            for user_id, ratings in user_ratings.items():
                if ratings:
                    if metric == 'average':
                        value = self.get_average(ratings)
                    elif metric == 'median':
                        value = self.get_median(ratings)
                    else:
                        value = self.get_average(ratings)
                    if value is not None:
                        user_metrics[user_id] = round(value, 2)

            sorted_metrics = dict(sorted(user_metrics.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_metrics.items())[:n])
        
        def top_by_variance(self, n, min_num_of_ratings=5):
            """Топ-n пользователей с наибольшей дисперсией оценок"""
            user_ratings = {}
            for i in range(len(self.ratings_instance.userIds)):
                user_id = self.ratings_instance.userIds[i]
                rating = self.ratings_instance.ratings[i]
                if user_id not in user_ratings:
                    user_ratings[user_id] = []
                user_ratings[user_id].append(rating)

            user_variances = {}
            for user_id, ratings in user_ratings.items():
                if len(ratings) >= min_num_of_ratings and len(ratings) > 1:
                    try:
                        variance = statistics.variance(ratings)
                        user_variances[user_id] = round(variance, 2)
                    except statistics.StatisticsError:
                        continue
            
            sorted_variances = dict(sorted(user_variances.items(), key=lambda x: x[1], reverse=True))
            return dict(list(sorted_variances.items())[:n])


class Tags:
    """
    Analyzing data from tags.csv
    """
    def __init__(self, path_to_the_file):
        self.tags = []
        self.user_tags = {}
        self.load_tags(path_to_the_file)

    def load_tags(self, path_to_the_file):
        """Загрузка тегов из файла"""
        if not os.path.isfile(path_to_the_file):
            print(f"File not found: {path_to_the_file}")
            return
        try:
            with open(path_to_the_file, mode='r', encoding='utf-8') as file:
                next(file)
                for line in file:
                    parts = line.strip().split(',')
                    tag = parts[2].strip()
                    self.tags.append(tag)
                    user_id = parts[0].strip()
                    # Считаем теги по пользователям
                    if user_id not in self.user_tags:
                        self.user_tags[user_id] = 1
                    else:
                        self.user_tags[user_id] += 1
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
        
    def most_words(self, n):
        """Теги с наибольшим количеством слов."""
        # посчитать, сколько слов в каждом теге
        tag_word_counts = {}
        for tag in set(self.tags):
            words = tag.split()
            word_count = len(words)
            tag_word_counts[tag] = word_count

        # отсортировать теги по количеству слов (от большего к меньшему)
        sorted_items = sorted(tag_word_counts.items(), key=lambda x: x[1], reverse=True)

        # взять первые n тегов
        result = {}
        for i in range(min(n, len(sorted_items))):
            tag, count = sorted_items[i]
            result[tag] = count

        return result

    def longest(self, n):
        """Самые длинные теги по количеству символов."""
        # получить уникальные теги
        unique_tags = list(set(self.tags))

        # отсортировать по длине (от самого длинного к короткому)
        for i in range(len(unique_tags)):
            for j in range(i + 1, len(unique_tags)):
                if len(unique_tags[i]) < len(unique_tags[j]):
                    unique_tags[i], unique_tags[j] = unique_tags[j], unique_tags[i]

        sorted_tags = sorted(unique_tags, key=len, reverse=True)

        result = []
        for i in range(min(n, len(sorted_tags))):
            result.append(sorted_tags[i])

        return result

    def most_words_and_longest(self, n):
        """Теги, которые одновременно содержат много слов и являются длинными."""
        # Получаем два списка/словаря
        words_dict = self.most_words(n)      # словарь: тег -> кол-во слов
        longest_list = self.longest(n)       # список тегов

        # Преобразуем в множества для сравнения
        words_set = set(words_dict.keys())
        longest_set = set(longest_list)

        # Находим общие теги
        common_tags = []
        for tag in words_set:
            if tag in longest_set:
                common_tags.append(tag)
        
        common_tags = sorted(common_tags)
        return common_tags

    def most_popular(self, n):
        """Самые популярные теги (чаще всего используемые)."""
        # Считаем, сколько раз встречается каждый тег
        tag_counts = {}
        for tag in self.tags:
            if tag in tag_counts:
                tag_counts[tag] += 1
            else:
                tag_counts[tag] = 1

        # Преобразуем в список пар (тег, частота) и сортируем по частоте
        items = list(tag_counts.items())
        items.sort(key=lambda x: x[1], reverse=True)

        # Берём первые n
        result = {}
        for i in range(min(n, len(items))):
            tag, count = items[i]
            result[tag] = count

        return result

    def tags_with(self, word):
        """Теги, содержащие указанное слово."""
        # Собираем уникальные теги, содержащие слово
        found_tags = []
        seen = set()  # чтобы избежать дубликатов

        for tag in self.tags:
            if word in tag and tag not in seen:
                found_tags.append(tag)
                seen.add(tag)

        # Сортируем по алфавиту
        found_tags.sort()
        return found_tags
    
    def count_tags_by_user(self, n):
        """Самые активные пользователи по количеству тегов."""
        # self.user_tags — уже словарь {user: count}
        items = list(self.user_tags.items())

        # Сортируем по количеству тегов (по убыванию)
        items.sort(key=lambda x: x[1], reverse=True)

        # Берём первые n
        result = {}
        for i in range(min(n, len(items))):
            user, count = items[i]
            result[user] = count

        return result


class Movies:
    """
    Analyzing data from movies.csv
    """
    def __init__(self, path_to_the_file):
        self.movies = []
        self.load_movies(path_to_the_file)

    def load_movies(self, path_to_the_file):
        if not os.path.isfile(path_to_the_file):
            print(f"File not found: {path_to_the_file}")
            return
        try:
            with open(path_to_the_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for line in lines[1:]:
                    # Парсим CSV с учетом кавычек
                    parts = []
                    current = ""
                    in_quotes = False
                    
                    for char in line.strip():
                        if char == '"':
                            in_quotes = not in_quotes
                        elif char == ',' and not in_quotes:
                            parts.append(current)
                            current = ""
                        else:
                            current += char
                    if current:
                        parts.append(current)
                    
                    if len(parts) >= 3:
                        movie_id = parts[0].strip()
                        title = parts[1].strip()
                        genres = parts[2].strip()
                        self.movies.append([movie_id, title, genres])
                    
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")

    def dist_by_release(self):
        """Подсчитывает количество фильмов по годам выпуска."""
        release_years = Counter()
        for movie in self.movies:
            match = re.search(r'\((\d{4})\)', movie[1])
            if match:
                release_years[match.group(1)] += 1
        release_years = dict(sorted(release_years.items(), key=lambda x: x[1], reverse=True))
        return release_years

    def dist_by_genres(self):
        """Подсчитывает количество фильмов по жанрам."""
        genres = Counter()
        for movie in self.movies:
            genre_list = movie[2].split('|')
            for genre in map(str.strip, genre_list):
                genres[genre] += 1
        genres = dict(genres.most_common())
        return genres

    def most_genres(self, n):
        """Находит фильмы с наибольшим количеством жанров."""
        genres_count = {movie[1]: len(movie[2].split('|')) for movie in self.movies}
        movies = dict(sorted(genres_count.items(), key=lambda x: x[1], reverse=True)[:n])
        return movies

    def movies_with(self, word):
        """Ищет фильмы, содержащие указанное слово в названии."""
        unique_movies = set()
        for movie in self.movies:
            title = movie[1]
            if word.lower() in title.lower():
                unique_movies.add(title)
        sorted_movies = sorted(unique_movies)
        return sorted_movies
    

class Links:
    """
    Analyzing data from links.csv
    """
    def __init__(self, path_to_the_file):
        self.path_to_the_file = path_to_the_file
        self.data = self.load_data(path_to_the_file)
        self.imdb_data = []
        
        self.movies_data = self._load_movies_data()
        self.imdb_to_title = self._create_imdb_to_title_mapping()
    
    def _load_movies_data(self):
        """Загружаем данные о фильмах для сопоставления"""
        movies_data = {}
        
        links_dir = os.path.dirname(self.path_to_the_file)
        possible_paths = [
            os.path.join(links_dir, 'movies.csv'),
            os.path.join(links_dir, 'ml-latest-small/movies.csv'),
            'ml-latest-small/movies.csv',
            os.path.join(os.path.dirname(__file__), 'ml-latest-small/movies.csv'),
            'movies.csv'
        ]
        
        for movies_path in possible_paths:
            if os.path.exists(movies_path):
                try:
                    print(f"Загружаем фильмы из: {movies_path}")
                    with open(movies_path, 'r', encoding='utf-8') as file:
                        next(file)
                        for line in file:
                            # Парсим ручками для обработки кавычек
                            if '"' in line:
                                parts = line.strip().split(',"')
                                if len(parts) >= 2:
                                    movie_id = parts[0].strip()
                                    rest = '"'.join(parts[1:])
                                    title_genres = rest.rsplit('",', 1)
                                    if len(title_genres) >= 1:
                                        title = title_genres[0].strip('"')
                                        movies_data[int(movie_id)] = title
                            else:
                                parts = line.strip().split(',')
                                if len(parts) >= 2:
                                    movie_id = parts[0].strip()
                                    title = parts[1].strip()
                                    movies_data[int(movie_id)] = title
                    print(f"Загружено {len(movies_data)} фильмов")
                    break
                except Exception as e:
                    print(f"Ошибка при загрузке {movies_path}: {e}")
                    continue
        
        if not movies_data:
            print("Файл movies.csv не найден. Будут использоваться только movieId.")
        
        return movies_data
    
    def _create_imdb_to_title_mapping(self):
        """Создаем сопоставление imdbId -> название фильма"""
        imdb_to_title = {}
        
        for row in self.data:
            movie_id = row.get('movieId')
            imdb_id = row.get('imdbId', '')
            
            if movie_id and imdb_id:
                try:
                    # Преобразуем movieId в int для поиска в movies_data
                    movie_id_int = int(movie_id)
                    title = self.movies_data.get(movie_id_int)
                    
                    if title:
                        # Очищаем imdbId (убираем 'tt' если есть)
                        clean_imdb_id = imdb_id.replace('tt', '')
                        imdb_to_title[clean_imdb_id] = title
                    else:
                        # Если название не найдено, используем заглушку
                        clean_imdb_id = imdb_id.replace('tt', '')
                        imdb_to_title[clean_imdb_id] = f"Movie {movie_id}"
                        
                except ValueError:
                    continue
        
        return imdb_to_title
    
    def read_csv_column(self, file_path, column_name):
        values = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                headers = file.readline().strip().split(',')
                if column_name not in headers:
                    raise ValueError(f"Column '{column_name}' not found in headers.")
                column_index = headers.index(column_name)
                for line in file:
                    row = line.strip().split(',')
                    if len(row) > column_index:
                        value = row[column_index].strip()
                        values.append(value)
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
        return values
    
    def load_data(self, path_to_file):
        """Load data from a text file formatted as CSV."""
        data = []
        with open(path_to_file, mode='r', encoding='utf-8') as file:
            headers = file.readline().strip().split(',')
            for line in file:
                values = line.strip().split(',')
                row = {}
                for i in range(len(headers)):
                    row[headers[i]] = values[i] if i < len(values) else ''
                data.append(row)
        return data
    
    def get_imdb(self, list_of_movies, list_of_fields):
        """
        The method returns a list of lists [movieId, field1, field2, field3, ...] for the list of movies given as the argument (movieId).
        For example, [movieId, Director, Budget, Cumulative Worldwide Gross, Runtime].
        The values should be parsed from the IMDB webpages of the movies.
        Sort it by movieId descendingly.
        """
        imdb_info = []
        for imdb_id in list_of_movies:
            # Очищаем imdb_id (убираем 'tt' если есть)
            clean_imdb_id = str(imdb_id).replace('tt', '')
            
            # Получаем название фильма
            title = self.imdb_to_title.get(clean_imdb_id, f"Movie {clean_imdb_id}")
            
            imdb_url = f"https://www.imdb.com/title/tt{clean_imdb_id}/?ref_=vp_vi_tt"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
            try:
                response = requests.get(imdb_url, headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Сохраняем clean_imdb_id и title
                movie_data = [clean_imdb_id, title]
                
                for field in list_of_fields:
                    if field == "Director":
                        director = self.extract_director(soup) or "Unknown"
                        movie_data.append(director)
                    elif field == "Budget":
                        budget = self.extract_budget(soup) or "N/A"
                        movie_data.append(budget)
                    elif field == "Cumulative Worldwide Gross":
                        gross = self.extract_gross(soup) or "N/A"
                        movie_data.append(gross)
                    elif field == "Runtime":
                        runtime = self.extract_runtime(soup) or "N/A"
                        movie_data.append(runtime)
                    else:
                        movie_data.append("N/A")
                
                imdb_info.append(movie_data)

            except requests.RequestException as e:
                print(f"Error fetching data for movie ID {clean_imdb_id}: {e}")
                # Добавляем запись с значениями по умолчанию
                imdb_info.append([clean_imdb_id, title] + ["N/A"] * len(list_of_fields))

        # Сортируем по imdb_id (как строке)
        imdb_info.sort(key=lambda x: x[0], reverse=True)
        self.imdb_data = imdb_info
        return imdb_info
    
    def _get_title_by_id(self, movie_id):
        """Получить название фильма по movieId или imdbId"""
        # Пробуем найти по movieId (если это число)
        try:
            movie_id_int = int(movie_id)
            return self.movies_data.get(movie_id_int, f"Movie {movie_id}")
        except ValueError:
            # Если movie_id не число, ищем по imdbId в joined_data
            for row in self.joined_data:
                if row.get('imdbId') == movie_id or row.get('imdbId') == f"tt{movie_id}":
                    return row.get('title', f"Movie {movie_id}")
            return f"Movie {movie_id}"
    
    def extract_director(self, soup):
        """Найти режиссера на странице."""
        try:
            director_tag = soup.find('a', href=re.compile(r'/name/nm\d+/'))
            return director_tag.text.strip() if director_tag else None
        except AttributeError:
            return None

    def extract_budget(self, soup):
        """Найти бюджет."""
        try:
            budget_tag = soup.find(string='Budget')
            return budget_tag.find_next().text.strip() if budget_tag else None
        except AttributeError:
            return None

    def extract_gross(self, soup):
        """Найти мировые сборы."""
        try:
            gross_tag = soup.find(string='Gross worldwide')
            return gross_tag.find_next().text.strip() if gross_tag else None
        except AttributeError:
            return None

    def extract_runtime(self, soup):
        """Найти продолжительность."""
        try:
            runtime_tag = soup.find(string='Runtime')
            return runtime_tag.find_next().text.strip() if runtime_tag else None
        except AttributeError:
            return None
        
    def extract_rating(self, soup):
        """Найти оценку."""
        try:
            rating_tag = soup.find('span', {'class': 'sc-bde20123-1'})
            if rating_tag:
                return rating_tag.text.strip()
            
            # Alternative way to find rating
            rating_text = soup.find(string='IMDb RATING')
            if rating_text:
                rating_element = rating_text.find_next()
                if rating_element:
                    return rating_element.text.strip()[:6]
            
            return None
        except AttributeError:
            return None
        
    def top_directors(self, n):
        """
        The method returns a dict with top-n directors where the keys are directors and 
        the values are numbers of movies created by them. Sort it by numbers descendingly.
        """
        director_count = {}

        for movie in self.imdb_data:
            director_name = movie[2]  # Индекс 2, так как теперь: [movie_id, title, director, ...]
            if director_name and director_name != "Unknown":
                if director_name in director_count:
                    director_count[director_name] += 1
                else:
                    director_count[director_name] = 1
        
        sorted_directors = dict(sorted(director_count.items(), key=lambda item: item[1], reverse=True))
        directors = dict(list(sorted_directors.items())[:n])
        return directors
        
    def most_expensive(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are their budgets. Sort it by budgets descendingly.
        """
        budgets = []
        for movie in self.imdb_data:
            # movie[0] - imdb_id, movie[1] - title, movie[3] - budget
            if len(movie) > 3:
                title = movie[1]
                budget_str = movie[3]
                budget = self.parse_budget(budget_str)
                if title and budget > 0:
                    budgets.append((title, budget))
        
        budgets.sort(key=lambda x: x[1], reverse=True)
        return dict(budgets[:n])
    
    def most_profitable(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are the difference between cumulative worldwide gross and budget.
        Sort it by the difference descendingly.
        """
        profits = []
        for movie in self.imdb_data:
            if len(movie) > 4:
                title = movie[1]
                budget_str = movie[3]
                gross_str = movie[4]
                budget = self.parse_budget(budget_str)
                gross = self.parse_budget(gross_str)
                profit = gross - budget
                if title:
                    profits.append((title, profit))
        
        profits.sort(key=lambda x: x[1], reverse=True)
        return dict(profits[:n])
    
    def longest(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are their runtime. If there are more than one version – choose any.
        Sort it by runtime descendingly.
        """
        runtimes = []
        for movie in self.imdb_data:
            if len(movie) > 5:
                title = movie[1]
                runtime_str = movie[5]
                runtime = self.parse_runtime(runtime_str)
                if title and runtime > 0:
                    runtimes.append((title, runtime))
        
        runtimes.sort(key=lambda x: x[1], reverse=True)
        return dict(runtimes[:n])
    
    def top_cost_per_minute(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are the budgets divided by their runtime. The budgets can be in different currencies – do not pay attention to it. 
        The values should be rounded to 2 decimals. Sort it by the division descendingly.
        """
        costs = []
        for movie in self.imdb_data:
            if len(movie) > 5:
                title = movie[1]
                budget_str = movie[3] if len(movie) > 3 else "N/A"
                runtime_str = movie[5] if len(movie) > 5 else "N/A"
                budget = self.parse_budget(budget_str)
                runtime = self.parse_runtime(runtime_str)
                if title and runtime > 0:
                    cost_per_minute = budget / runtime
                    costs.append((title, round(cost_per_minute, 2)))
        
        costs.sort(key=lambda x: x[1], reverse=True)
        return dict(costs[:n])

    def parse_budget(self, budget_str):
        """Преобразовать строку с деньгами в число."""
        if budget_str and budget_str != "N/A":
            numeric_str = re.sub(r'[^\d]', '', budget_str) 
            try:
                return float(numeric_str)
            except ValueError:
                return 0.0
        return 0.0

    def parse_runtime(self, runtime_str):
        """Преобразовать время в минуты."""
        if runtime_str and runtime_str != "N/A":
            match = re.search(r'(\d+)\s*h[s]?\s*(\d+)?\s*m[s]?', runtime_str)
            if match:
                hours = int(match.group(1))
                minutes = int(match.group(2)) if match.group(2) else 0
                return hours * 60 + minutes
        return 0      

    def get_imdb_rating(self, list_of_movies):
        """Получить рейтинги для списка фильмов."""
        rating_info = []
        
        for movie_id in list_of_movies:
            imdb_url = f"https://www.imdb.com/title/tt{movie_id}/ratings/?ref_=tt_ov_rat"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
            try:
                response = requests.get(imdb_url, headers=headers)
                response.raise_for_status()  
                soup = BeautifulSoup(response.content, 'html.parser')
                
                title = self._get_title_by_id(movie_id)
                movie_rating_data = [movie_id, title]
                rating = self.extract_rating(soup) or "N/A"
                movie_rating_data.append(rating)
                
                rating_info.append(movie_rating_data)
                
            except requests.RequestException as e:
                print(f"Error fetching data for movie ID {movie_id}: {e}")
                title = self._get_title_by_id(movie_id)
                rating_info.append([movie_id, title, "N/A"])

        rating_info.sort(key=lambda x: x[0], reverse=True)
        self.rating_data = rating_info
        return rating_info

    
class Tests:
    #--------------------------------BASIS-------------------------------------------------
    @classmethod
    def setup_class(cls):
        """Устанавливает тестовые данные для всех классов"""

        cls.movies_file = cls._create_movies_csv()
        cls.links_file = cls._create_links_csv()
        cls.ratings_file = cls._create_ratings_csv()
        cls.tags_file = cls._create_tags_csv() 

    @classmethod
    def _create_movies_csv(cls):
        """Создаёт csv для класса Movies"""
        
        temp_file_movies = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.csv',
            delete=False,
            encoding='utf-8'
        )

        writer = csv.writer(temp_file_movies)
        writer.writerow(['movieId','title','genres'])
        test_data = [
            [1, 'Toy Story (1995)', 'Animation|Comedy'],
            [2, 'Jumanji (1995)', 'Adventure|Fantasy'],
            [3, 'Grumpier Old Men (1995)', 'Comedy|Romance'],
            [4, 'Waiting to Exhale (1995)', 'Comedy|Drama|Romance'],
            [5, 'Father of the Bride Part II (1995)', 'Comedy'],
            [6, 'Heat (1995)', 'Action|Crime|Thriller'],
            [7, 'Sabrina (1995)', 'Comedy|Romance'],
            [8, 'Tom and Huck (1995)', 'Adventure|Children'],
            [9, 'Sudden Death (1995)', 'Action'],
            [10, 'GoldenEye (1995)', 'Action|Adventure|Thriller'],
            [11, 'Love Story (1970)', 'Drama|Romance'],
            [12, 'Movie Without Year', 'Drama'],
            [13, 'Another Love (1999)', 'Romance'],
            [14, 'Story of My Life (2000)', 'Drama'],
        ]
        writer.writerows(test_data)

        temp_file_movies.close()
        return temp_file_movies
    
    @classmethod
    def _create_links_csv(cls):
        temp_file_links = tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='_links.csv', 
            delete=False, 
            encoding='utf-8')
        
        writer = csv.writer(temp_file_links)
        writer.writerow(['movieId', 'imdbId', 'tmdbId'])

        cls.test_links_data = [
        [1, '0114709', '862'],
        [2, '0113497', '8844'],
        [3, '0113228', ''],
        [4, '', '12345'],
        [5, 'tt1234567', '999'],
        [999, '9999999', '777'],
    ]

        writer.writerows(cls.test_links_data)
        temp_file_links.close()
        return temp_file_links

    @classmethod
    def _create_ratings_csv(cls):
        temp_file_ratings = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.csv',
            delete=False,
            encoding='utf-8'
        )

        writer = csv.writer(temp_file_ratings)
        writer.writerow(['userId', 'movieId', 'rating', 'timestamp'])
        
        test_data = [
            [1, 1, 4.0, 946684800],
            [1, 2, 3.0, 946771200],
            [2, 1, 5.0, 946857600],
            [2, 2, 2.0, 946944000],
            [3, 1, 4.0, 978307200],
            [3, 2, 1.0, 978393600],
            [4, 3, 3.5, 1009843200],
            [4, 4, 4.5, 1009929600],
            [5, 5, 2.0, 1010016000],
            [6, 1, 3.0, 1010102400],
            [6, 2, 4.0, 1010188800],
            [7, 1, 1.0, 1010275200],
            [7, 3, 5.0, 1010361600],
            [8, 4, 2.5, 1010448000],
            [9, 5, 4.0, 1010534400],
            [10, 6, 3.0, 1010620800],
        ]
        
        writer.writerows(test_data)
        temp_file_ratings.close()
        
        cls.test_ratings_data = test_data
        return temp_file_ratings

    @classmethod
    def _create_tags_csv(cls):
        """Создаёт csv для класса Tags"""
        temp_file_tags = tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.csv',
            delete=False,
            encoding='utf-8'
        )
        
        writer = csv.writer(temp_file_tags)
        writer.writerow(['userId', 'movieId', 'tag', 'timestamp'])
        
        test_data = [
            [1, 1, 'animation', 946684800],
            [1, 2, 'adventure', 946771200],
            [2, 1, 'comedy', 946857600],
            [2, 2, 'fantasy', 946944000],
            [3, 1, 'funny', 978307200],
            [3, 2, 'children', 978393600],
            [4, 3, 'action', 1009843200],
            [5, 1, 'Pixar animation', 1010102400],
            [5, 2, 'family movie', 1010188800],
            [6, 1, 'great film', 1010275200],
            [7, 1, 'classic', 1010361600],
        ]
        
        writer.writerows(test_data)
        temp_file_tags.close()
        cls.test_tags_data = test_data
        return temp_file_tags
    
    def setup_method(self):
        self.movies = Movies(self.__class__.movies_file.name)
        self.links = Links(self.__class__.links_file.name)

        self.ratings = Ratings(self.__class__.ratings_file.name)
        self.ratings_movies = self.ratings.Movies(self.ratings)
        self.ratings_users = self.ratings.Users(self.ratings)

        self.tags = Tags(self.__class__.tags_file.name)


    #--------------------------------------------------------------------------------------

    #--------------------------------HELPER METHODS-------------------------------------------------
    def _assert_dict_types(self, result, key_type, value_type):
        """Проверяет, что результат - dict и его ключи/значения правильных типов"""
        # 111111111111111 - проверка типа возвращаемого значения
        assert isinstance(result, dict)

        # 222222222222222 - проверка типов элементов словаря
        for key, value in result.items():
            if key_type is not None:
                assert isinstance(key, key_type)
            
            if value_type is not None:
                assert isinstance(value, value_type)

    def _assert_list_types(self, result, element_type):
        """Проверяет, что результат - list и его ключи/значения правильных типов"""
        # 111111111111111 - проверка типа возвращаемого значения
        assert isinstance(result, list)

        # 222222222222222 - проверка типов элементов словаря
        for element in result:
            assert isinstance(element, element_type)

    def _assert_sorted_descending(self, values):
        """Проверка сортировки по убыванию"""
        for i in range(len(values) - 1):
            assert values[i] >= values[i + 1]

    def _assert_sorted_ascending(self, values):
        """Проверка сортировки по убыванию"""
        for i in range(len(values) - 1):
            assert values[i] <= values[i + 1]

    def _setup_imdb_mock(self, mock_get, movie_response):
        def side_effect(url, *args, **kwargs):
            for movie_id, content in movie_response.items():
                if f'tt{movie_id}/' in url:
                    mock_response = Mock()
                    mock_response.content = content
                    mock_response.raise_for_status = Mock()
                    return mock_response
            mock_response = Mock()
            mock_response.content = b'<html></html>'
            mock_response.raise_for_status = Mock()
            return mock_response
        
        mock_get.side_effect = side_effect

    #-----------------------------------------------------------------------------------------------
    
    #-----------------For Movies----------------------
    def test_movies_load_movies(self):
        #111
        assert isinstance(self.movies.movies, list)
        
        #222 - проверяем структуру каждого фильма
        for movie in self.movies.movies:
            assert isinstance(movie, list)
            assert len(movie) == 3

            assert isinstance(movie[0], str)
            assert isinstance(movie[1], str)
            assert isinstance(movie[2], str)

    def test_movie_dist_by_release(self):
        result = self.movies.dist_by_release()

        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)

        #3333333333333333333333
        years = list(result.values())
        self._assert_sorted_descending(years)

    def test_movie_dist_by_genres(self):
        result = self.movies.dist_by_genres()

        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)

        #3333333333333333333333
        counts = list(result.values())
        self._assert_sorted_descending(counts)

    def test_movie_most_genres(self):
        n = 5
        result = self.movies.most_genres(n)

        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)

        #3333333333333333333333
        counts = list(result.values())
        self._assert_sorted_descending(counts)

        #4444444444444444444444 - возвращает задаваемое количество фильмов
        assert len(result) <= n

    def test_movie_movies_with(self):
        word = 'love'
        result = self.movies.movies_with(word)

        #111 + 222
        self._assert_list_types(result, element_type=str)

        #3333333333333333333333
        self._assert_sorted_ascending(result)

        #4444444444444444444444 - все названия содержат слово
        for title in result:
            assert word.lower() in title.lower()


    #-----------------For Links----------------------
    def test_links_read_csv_column(self):
        column = 'movieId'
        result = self.links.read_csv_column(self.__class__.links_file.name, column)

        #111 + 222
        self._assert_list_types(result, element_type=str)

        #333
        headers = ['movieId', 'imdbId', 'tmdbId']
        column_index = headers.index(column)
        
        expected_values = [str(row[column_index]) for row in self.__class__.test_links_data]
        
        assert result == expected_values

    def test_links_load_data(self):
        result = self.links.load_data(self.__class__.links_file.name)

        #111 + 222
        self._assert_list_types(result, dict)

        #444 проверка на совпадение всех строк
        expected_data = []
        for row in self.__class__.test_links_data:
            expected_data.append({
                'movieId': str(row[0]),
                'imdbId': str(row[1]),
                'tmdbId': str(row[2])
            })

        assert result == expected_data

    @patch('requests.get')
    def test_links_get_imdb(self, mock_get):
        movie_response = {
        1: b'''
            <html>
                <body>
                    <a href="/name/nm1234567/">John Lasseter</a>
                    <li><label>Budget:</label><span>$30,000,000</span></li>
                    <li><label>Gross worldwide:</label><span>$373,554,033</span></li>
                    <li><label>Runtime:</label><span>1h 21min</span></li>
                </body>
            </html>
        ''',
        2: b'''
            <html>
                <body>
                    <a href="/name/nm9876543/">Joe Johnston</a>
                    <li><label>Budget:</label><span>$65,000,000</span></li>
                    <li><label>Gross worldwide:</label><span>$262,797,249</span></li>
                    <li><label>Runtime:</label><span>1h 44min</span></li>
                </body>
            </html>
        ''',
        11: b'''
            <html>
                <body>
                    <a href="/name/nm5555555/">Arthur Hiller</a>
                    <li><label>Budget:</label><span>$2,200,000</span></li>
                    <li><label>Runtime:</label><span>1h 40min</span></li>
                </body>
            </html>
        '''}

        self._setup_imdb_mock(mock_get, movie_response)
        list_of_movies = [1, 2, 11]
        list_of_fields = ['Director', 'Budget', 'Cumulative Worldwide Gross', 'Runtime']
        result = self.links.get_imdb(list_of_movies, list_of_fields)

        #111 + 222
        self._assert_list_types(result, list)

        #333
        movie_ids = [item[0] for item in result]
        self._assert_sorted_descending(movie_ids)
     
    def test_links_top_directors(self):
        n = 2
        result = self.links.top_directors(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)
        
        #333
        counts = list(result.values())
        self._assert_sorted_descending(counts)

    def test_links_most_expensive(self):
        n = 2
        result = self.links.most_expensive(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=(int, float))
        
        #333
        budgets = list(result.values())
        self._assert_sorted_descending(budgets)
    
    def test_links_most_profitable(self):
        n = 2
        result = self.links.most_profitable(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=(int, float))
        
        #333
        profits = list(result.values())
        self._assert_sorted_descending(profits)

    def test_links_longest(self):
        n = 2
        result = self.links.longest(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=int)
        
        #333
        runtimes = list(result.values())
        self._assert_sorted_descending(runtimes)

    def test_links_top_cost_per_minute(self):
        n = 2
        result = self.links.top_cost_per_minute(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=(int, float))
        
        #333
        costs = list(result.values())
        self._assert_sorted_descending(costs)

    #-----------------For Ratings.Movies----------------------
    def test_ratings_load_data(self):        
        # 11111
        assert isinstance(self.ratings.userIds, list)
        assert isinstance(self.ratings.movieIds, list)
        assert isinstance(self.ratings.ratings, list)
        assert isinstance(self.ratings.timestamps, list)
        assert isinstance(self.ratings.years, list)
        
        # 2222
        for user_id in self.ratings.userIds:
            assert isinstance(user_id, int)
        
        for movie_id in self.ratings.movieIds:
            assert isinstance(movie_id, int)
        
        for rating in self.ratings.ratings:
            assert isinstance(rating, float)
        
        for timestamp in self.ratings.timestamps:
            assert isinstance(timestamp, int)
        
        for year in self.ratings.years:
            assert isinstance(year, int)

    def test_ratings_movies_dist_by_year(self):
        result = self.ratings_movies.dist_by_year()
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=int)
        
        #333
        years = list(result.keys())
        self._assert_sorted_ascending(years)

    def test_ratings_movies_dist_by_rating(self):
        result = self.ratings_movies.dist_by_rating()
        
        #111 + 222
        self._assert_dict_types(result, key_type=float, value_type=int)
        
        #333
        ratings = list(result.keys())
        self._assert_sorted_ascending(ratings)

    def test_ratings_movies_top_by_num_of_ratings(self):
        n = 3
        result = self.ratings_movies.top_by_num_of_ratings(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)
        
        #333
        counts = list(result.values())
        self._assert_sorted_descending(counts)
        
    def test_ratings_movies_top_by_ratings_average(self):
        n = 3
        result = self.ratings_movies.top_by_ratings(n, metric='average')
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=float)
        
        #333
        averages = list(result.values())
        self._assert_sorted_descending(averages)

    def test_ratings_movies_top_by_ratings_median(self):
        n = 3
        result = self.ratings_movies.top_by_ratings(n, metric='median')
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=float)
        
        #333
        medians = list(result.values())
        self._assert_sorted_descending(medians)
        
    def test_ratings_movies_top_controversial(self):
        n = 2
        min_num_of_ratings = 2
        result = self.ratings_movies.top_controversial(n, min_num_of_ratings)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=float)
        
        #333
        variances = list(result.values())
        self._assert_sorted_descending(variances)
        

    #-----------------For Ratings.Users----------------------
    def test_ratings_users_dist_by_num_of_ratings(self):
        """Test dist_by_num_of_ratings method"""
        result = self.ratings_users.dist_by_num_of_ratings()
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=int)
        
        #333
        user_ids = list(result.keys())
        self._assert_sorted_ascending(user_ids)

    def test_ratings_users_top_by_num_of_ratings(self):
        n = 3
        result = self.ratings_users.top_by_num_of_ratings(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=int)
        
        #333
        counts = list(result.values())
        self._assert_sorted_descending(counts)
        
    def test_ratings_users_top_by_ratings_average(self):
        n = 3
        result = self.ratings_users.top_by_ratings(n, metric='average')
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=float)
        
        #333
        averages = list(result.values())
        self._assert_sorted_descending(averages)  

    def test_ratings_users_top_by_ratings_median(self):
        n = 3
        result = self.ratings_users.top_by_ratings(n, metric='median')
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=float)
        
        #333
        medians = list(result.values())
        self._assert_sorted_descending(medians)     

    def test_ratings_users_top_by_variance(self):
        n = 2
        min_num_of_ratings = 2
        result = self.ratings_users.top_by_variance(n, min_num_of_ratings)
        
        #111 + 222
        self._assert_dict_types(result, key_type=int, value_type=float)
        
        #333
        variances = list(result.values())
        self._assert_sorted_descending(variances)
        
    #-----------------For Tags----------------------
    def test_tags_most_words(self):
        n = 3
        result = self.tags.most_words(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)
        
        #333
        word_counts = list(result.values())
        self._assert_sorted_descending(word_counts)

    def test_tags_longest(self):
        n = 3
        result = self.tags.longest(n)
        
        # 111 + 222
        self._assert_list_types(result, element_type=str)
        
        #333
        lengths = [len(tag) for tag in result]
        self._assert_sorted_descending(lengths)

    def test_tags_most_words_and_longest(self):
        n = 3
        result = self.tags.most_words_and_longest(n)
        
        # 111 + 222
        self._assert_list_types(result, element_type=str)
        
        # 333
        sorted_result = sorted(result)
        assert result == sorted_result

    def test_tags_most_popular(self):
        n = 3
        result = self.tags.most_popular(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)
        
        #333
        counts = list(result.values())
        self._assert_sorted_descending(counts)

    def test_tags_tags_with(self):
        word = 'anim'
        result = self.tags.tags_with(word)
        
        # 111 + 222
        self._assert_list_types(result, element_type=str)
        
        #333
        self._assert_sorted_ascending(result)

    def test_tags_count_tags_by_user(self):
        n = 3
        result = self.tags.count_tags_by_user(n)
        
        #111 + 222
        self._assert_dict_types(result, key_type=str, value_type=int)
        
        #333
        counts = list(result.values())
        self._assert_sorted_descending(counts)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
