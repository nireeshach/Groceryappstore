# Generated by Django 2.0.5 on 2018-05-24 09:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0006_auto_20180524_0950'),
    ]

    operations = [
        migrations.AddField(
            model_name='ingredient',
            name='isfinalspec',
            field=models.BooleanField(choices=[(True, 'Yes'), (False, 'No')], default=False),
        ),
    ]
