import string
import asyncio
from collections import defaultdict, Counter

import httpx
from matplotlib import pyplot as plt


# Отримаємо текст за наданим посиланням
async def get_text(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        if response.status_code == 200:
            return response.text
        else:
            return None


# Створимо функцію для видалення пунктуації і асинхронного Мапінгу
def map_function(text):
    text.translate(str.maketrans("", "", string.punctuation))
    words = text.split()
    return [(word, 1) for word in words]


# Створимо функцію для виконання Shuffle
def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


async def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


# Виконаємо пошук і формування списку слів за допомогою MapReduce
async def map_reduce(url):
    # Отримаємо текст з вебсайту
    text = await get_text(url)
    # Запускаємо Мапінг і обробку тексту
    mapped_result = map_function(text)

    # Виконаємо Shuffle використовуючи результати Мапінга
    shuffled_words = shuffle_function(mapped_result)

    # Зберемо разом отримані результати та підрахуємо частоти використання
    reduced_result = await asyncio.gather(
        *[reduce_function(values) for values in shuffled_words])

    # Повернемо результати розрахунків у main
    return dict(reduced_result)


# Позначимо, що нам потрібні 10 найчастіших слів та побудуємо графік
def visualize_top_words(result, top_n=10):
    top_words = Counter(result).most_common(top_n)
    # Розділимо дані на слова та їх частоти
    words, counts = zip(*top_words)
    # Створимо графік
    plt.figure(figsize=(12, 9))
    plt.barh(words, counts, color="red")
    plt.xlabel("Частота використання")
    plt.ylabel("Слова")
    plt.title("10 найчастіше вживаних у тексті слів")
    plt.gca().invert_yaxis()
    plt.show()


if __name__ == '__main__':
    # Введемо посилання на текст для аналізу
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    # Проаналізуємо слова за частотою використання і запишемо результат
    res = asyncio.run(map_reduce(url))
    # Візуалізуємо отримані результати за допомогою графіку
    visualize_top_words(res)
