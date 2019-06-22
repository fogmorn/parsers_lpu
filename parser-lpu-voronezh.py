import time
from datetime import datetime as dt
from django.core.management.base import BaseCommand, CommandError

from grab.spider import Spider, Task

from goslpu.models import GosLpu, GosDoctor, GosSchedule


class ExampleSpider(Spider):
  base_url = 'https://rmis36.ru/er/ereg3/cities/280972/hospitals/'

  initial_urls = ['https://rmis36.ru/er/ereg3/cities/280972/hospitals/']

  # В случае отладки и тестирования, используем файл
  #
  # def start(self):
  #   data = open('voronezh/hospitals.html', 'rb').read()
  #   self.task_initial(Grab(data))
  #   return

  def task_initial(self, grab, task):
    # Разбор страницы с больницами
    for li in grab.doc.select('//li[@class="list-group-item mo"]'):
      url = self.base_url + li.select('./a[@class="mo-title"]').attr('href')

      if not url.endswith('javascript:void(0)'):
        name = li.select('.//span[@class="org"]').text()

        address = li.select(
          './/div[@class="address"]/text()').text(). \
          replace('Адрес: г. Воронеж, ', '').replace(',', '')

        site = li.select('.//div[@class="site"]/a', default=None).attr('href')

        city = 'Воронеж'

        lpu_obj = GosLpu(
          name=name,
          city=city,
          address=address)
        lpu_obj.save()

        yield Task('speciality', url=url, lpu=lpu_obj)

        # Для отладки/тестирования
        #
        # g = Grab()
        # g.go(url)
        # self.task_speciality(g, url, org)
    return

  def task_speciality(self, grab, task):
    # Разбор страницы со специальностями
    for lnk in grab.doc.select('//ul[@class="list-unstyled flat-list"]//a/@href'):
      url = str(task.url) + lnk.text()

      yield Task('schedule', url=url, lpu=task.lpu_obj)

      # Для отладки/тестирования
      #
      # g = Grab()
      # g.go(url + lnk.text())
      # self.task_schedule(g, org)
    return

  def task_schedule(self, grab, task):
    # Разбор страницы расписания врачей
    table = grab.doc.select('//table[@class="timetable doctors"]')

    # Дни недели
    month_dict = {
      "января": "01", "февраля": "02", "марта": "03", "апреля": "04",
      "мая": "05", "июня": "06", "июля": "07", "августа": "08",
      "сентября": "09", "октября": "10", "ноября": "11", "декабря": "12"}
    days = []
    data = table.select('./thead/tr/td')[2:]

    for i in range(0, 6):
      d = data[i].select('./div/following-sibling::text()').text().split()
      d[1] = month_dict[d[1]]

      d.append(str(dt.now().year))
      days.append(dt.strptime('.'.join(d), '%d.%m.%Y').strftime('%Y-%m-%d'))


    # Данные о враче и расписании приёма
    for tr in table.select('./tbody/tr'):
      data = tr.select('./td')

      fio = data[1].select('./strong').text().split(maxsplit=2)
      surname = fio[0]
      try:
        firstname = fio[1]
      except IndexError:
        firstname = None

      try:
        secondname = fio[2]
      except IndexError:
        secondname = None

      speciality = data[1].select('.//b/following-sibling::text()').text()

      doctor_obj = GosDoctor(
        surname=surname,
        firstname=firstname,
        secondname=secondname,
        speciality=speciality)
      doctor_obj.save()

      schedule_objects = []
      info = data[2:9]
      for i in range(0, 6):
        try:
          t = info[i].select('.//span').text().split(' - ')
        except:
          t = [None, None]

        # В случае, если есть только время начала приёма.
        if len(t) != 2:
          t = [None, None]

        k = GosSchedule(
          doctor_id=doctor_obj,
          gos_lpu_id=task.lpu_obj,
          date=days[i],
          time_start=t[0],
          time_end=t[1])
        schedule_objects.append(k)
      GosSchedule.objects.bulk_create(schedule_objects)

    time.sleep(10)
    return


class Command(BaseCommand):
  def handle(self, *args, **options):
    bot = ExampleSpider(thread_number=1)
    bot.run()
    return
