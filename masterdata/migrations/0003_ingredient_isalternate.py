# Generated by Django 2.0.5 on 2018-05-22 09:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0002_ingredient_alternates_new'),
    ]

    operations = [
        migrations.AddField(
            model_name='ingredient',
            name='isalternate',
            field=models.BooleanField(choices=[(True, 'Yes'), (False, 'No')], default=False),
        ),
    ]
