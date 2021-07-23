project('liburingpp', 'cpp', default_options : ['warning_level=3', 'cpp_std=c++17'])

liburingpp = library('liburingpp', 'src/iouring.cpp',
  include_directories : 'include')

liburingpp_dep = declare_dependency(
  include_directories : 'include',
  link_with : liburingpp)
