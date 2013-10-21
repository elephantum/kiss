import glob
import os.path

data_source = 'full_2013_10_16'

dirname = 'kiss.{data_source}'.format(data_source=data_source)
if not os.path.isdir(dirname): os.mkdir(dirname)

for filename_template in glob.glob('kiss.template/*.template.sql'):
	filename_res = os.path.join(dirname, os.path.basename(filename_template).replace('.template', ''))
	print 'processing', filename_template, '->', filename_res

	template_text = file(filename_template).read()

	file(filename_res, 'w+').write(template_text.format(data_source=data_source))