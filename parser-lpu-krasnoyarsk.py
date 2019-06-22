import time
import re
from datetime import datetime as dt, timedelta
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError

from grab.spider import Spider, Task

from goslpu.models import GosLpu, GosDoctor, GosSpeciality, GosSchedule


class ExampleSpider(Spider):
  initial_urls = ['https://web-registratura.ru/schedule']

  def task_initial(self, grab, task):
    for loc in grab.doc.select('//ul[@id="clinics"]/li/ul/li'):
      if re.match(r'г. Красноярск', loc.select('./div[1]').text()):
        url = loc.select('./a/@href').text()

        yield Task('clinic_schedule', url=self.initial_urls[0]+'/'+url)

  def task_clinic_schedule(self, grab, task):
    lpu_name = grab.doc.select('//div[@class="svc bg curved"]/h3').text()
    lpu_address = grab.doc.select('//div[@class="svc bg curved"]/div[1]').text()
    lpu_address = lpu_address.replace('г. Красноярск. ', '')

    lpu_obj = GosLpu(
        name=lpu_name,
        city='Красноярск',
        address=lpu_address)
    try:
      lpu_obj.save()
    except IntegrityError:
      lpu_obj = GosLpu.objects.get(name=lpu_name, address=lpu_address)


    for row in grab.doc.select('//tbody/tr'):
      info = row.select('./td')


      # Ячейка таблицы со специальностью
      if info[1].text() != '' and info[1].attr('title') == 'специальность':
        speciality = info[1].text().replace('врач - ', '')

        speciality_obj = GosSpeciality(name=speciality)
        try:
          speciality_obj.save()
        except IntegrityError:
          pass

      # Остальные ячейки
      elif info[1].text() != '':
        speciality_obj = GosSpeciality.objects.get(name=speciality)
        fio = info[1].text().split(', участок')[0]
        fio = fio.split(maxsplit=2)
        # debug
        try:
          surname = fio[0]
        except:
          print(lpu_name)
          print(fio)
          print(speciality)

        # В расписании, в поле "Врач" указывают "Анализ крови" и т.п.
        try:
          firstname = fio[1]
        except IndexError:
          firstname = None

        try:
          secondname = fio[2]
        except IndexError:
          secondname = None

        doctor_obj = GosDoctor(
          lpu_id=lpu_obj,
          speciality_id=speciality_obj,
          surname=surname,
          firstname=firstname,
          secondname=secondname)
        try:
          doctor_obj.save()
        except IntegrityError:
          doctor_obj = GosDoctor.objects.get(lpu_id=lpu_obj, surname=surname,
                                             firstname=firstname)

        # Получим даты текущей недели
        days = []
        first_dow = dt.now() - timedelta(days=dt.now().weekday())
        for i in range(0, 7):
          days.append((first_dow + timedelta(days=i)).strftime('%Y-%m-%d'))


        schedule_objects = []
        for i in range(2, 9):
          # Чётный день или нет
          d = 1
          if int(days[i-2].split('-')[2])%2 == 0:
            d = 2

          try:
            t = info[i].text().split('–')
            # Если в ячейке с временем приёма есть разделение на чёт/нечёт.
            try:
              t = info[i].select('./text()[' + str(d) + ']').text().split('–')
            except:
              pass
          except:
            t = [None, None]

          # Когда в ячейке вместо времени указывают "-",
          # в t остаётся пустая строка.
          if t[0] == '':
            t = [None, None]

          k = GosSchedule(
            doctor_id=doctor_obj,
            gos_lpu_id=lpu_obj,
            time_start=t[0],
            time_end=t[1],
            date=days[i-2])
          schedule_objects.append(k)
        GosSchedule.objects.bulk_create(schedule_objects)

    time.sleep(10)


class Command(BaseCommand):
  def handle(self, *args, **options):
    bot = ExampleSpider(thread_number=1)
    bot.run()
    return
