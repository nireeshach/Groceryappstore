# Generated by Django 2.0.5 on 2018-06-12 14:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0015_auto_20180612_1331'),
    ]

    operations = [
        migrations.AlterField(
            model_name='ingredient',
            name='size_finalspec',
            field=models.ManyToManyField(blank=True, related_name='size_finalspec', to='masterdata.Size'),
        ),
    ]