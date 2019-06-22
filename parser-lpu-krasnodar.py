import time
import re
from datetime import datetime as dt
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError

from grab.spider import Spider, Task

from goslpu.models import GosLpu, GosDoctor, GosSpeciality, GosSchedule
from doctors.models import Town
# from doctors.models import LpuAddress


class ExampleSpider(Spider):
  base_url = 'http://www.kmivc.ru/meduchrezhdeniya-goroda'
  town_obj = Town.objects.get(name='Краснодар')


  # initial_urls = ['http://www.kmivc.ru/meduchrezhdeniya-goroda/?page=1&lputype=1',
  #                 'http://www.kmivc.ru/meduchrezhdeniya-goroda/?page=2&lputype=1',
  #                 'http://www.kmivc.ru/meduchrezhdeniya-goroda/?page=3&lputype=1']

  # Для теста берём первую страницу
  initial_urls = ['http://www.kmivc.ru/meduchrezhdeniya-goroda/?page=1&lputype=1']

  # Для отладки (используем файл)
  #
  # def start(self):
  #   data = open('krasnodar/krasnodar-page-1.html', 'rb').read()
  #   self.task_initial(Grab(data))
  #   return

  def task_initial(self, grab, task):
    for li in grab.doc.select('//table[@cellpadding="4"]//li/a[2]'):
      lpu_address = re.match(r'.*Краснодар,?\s?(.*$)', li.attr('href')).group(1)
      lpu_url = li.select('following::a[1]').attr('href')
      lpu_name = li.select('preceding::a[1]').text()

      lpu_obj = GosLpu(
        town_id=self.town_obj,
        name=lpu_name,
        address=lpu_address)
      try:
        lpu_obj.save()
      except IntegrityError:
        print('Error lpu_obj')
        lpu_obj = GosLpu.objects.get(name=lpu_name, address=lpu_address)

      # yield Task('schedule', url=self.base_url+lpu_url, lpu_obj=lpu_obj)
      # debug
      #
      # url = base_url + url.text()
      # g = Grab()
      # g.go(url)
      # self.task_schedule(g)
    return

  def task_schedule(self, grab, task):
    # Даты в таблице расписания
    days = []
    data = grab.doc.select('//table[@class="big_table_rasp"]//tr[1]/th')[1:]

    for i in range(0, 7):
      d = data[i].text()[2:].split()
      d.append(str(dt.now().year))
      days.append(dt.strptime('.'.join(d), '%d.%m.%Y').strftime('%Y-%m-%d'))

    prev_doctor = [1, 2, 3]
    # Специальность, фио врача, расписание
    for row in grab.doc.select('//table[@class="big_table_rasp"]//tr'):
      info = row.select('.//td')

      if len(info) != 0:
        if info[0].attr('class') == 'spec_rasp':
          speciality = info[0].text().capitalize()

          speciality_obj = GosSpeciality(name=speciality)
          try:
            speciality_obj.save()
          except IntegrityError:
            pass

        else:
          speciality_obj = GosSpeciality.objects.get(name=speciality)
          surname = info[0].select('./b/text()')[0].text().capitalize()
          io = info[0].select('./b/text()')[1].text().split(maxsplit=1)
          try:
            firstname = io[0]
          except:
            firstname = None
          try:
            secondname = io[1]
          except:
            secondname = None


          # Проверяем, что врач это не дубль предыдущего врача в расписании.
          # Дубли врачей с временем приёма продолжительностью в 1 час были тут:
          # http://www.kmivc.ru/meduchrezhdeniya-goroda/?raspview=yes&idLpu=245
          #
          save = False
          if (prev_doctor[0] != surname and
                prev_doctor[1] != firstname and
                prev_doctor[2] != secondname):

            doctor_obj = GosDoctor(
              lpu_id=task.lpu_obj,
              speciality_id=speciality_obj,
              surname=surname,
              firstname=firstname,
              secondname=secondname)
            try:
              doctor_obj.save()
            except IntegrityError:
              doctor_obj = GosDoctor.objects.get(lpu_id=task.lpu_obj,
                                                 surname=surname,
                                                 firstname=firstname)
            prev_doctor = str(doctor_obj).split(' ')


            schedule_objects = []
            timetable = info[1:]
            for i in range(len(timetable)):
              x = timetable[i].select('./text()')

              # Время начала и время окончания приёма - 2 ячейки
              if len(x) == 2:
                t = [x[0].text(), x[1].text()]
              else:
                t = [None, None]

              k = GosSchedule(
                doctor_id=doctor_obj,
                gos_lpu_id=task.lpu_obj,
                time_start=t[0],
                time_end=t[1],
                date=days[i])
              schedule_objects.append(k)
              save = True

          if save:
            GosSchedule.objects.bulk_create(schedule_objects)
    time.sleep(5)
    return


class Command(BaseCommand):
  def handle(self, *args, **options):
    bot = ExampleSpider(thread_number=1)
    bot.load_proxylist(source='goslpu/proxy.txt', source_type='text_file', proxy_type='http')
    bot.run()
    return
